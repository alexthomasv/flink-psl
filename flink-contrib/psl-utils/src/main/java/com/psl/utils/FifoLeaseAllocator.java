package com.psl.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Allocates an exclusive lease over one of a small, pre-created set of FIFO pairs, without using
 * OS-level file locks. The lease is represented by an atomically created <code>.lease</code> file
 * (via {@link java.nio.file.StandardOpenOption#CREATE_NEW}), which makes the claim safe across
 * processes.
 *
 * <p><b>Intended use:</b> when only a fixed number (e.g., 2) of named FIFO pairs exist (e.g.,
 * <code>psl_fifo_{in,out}_0</code>, <code>psl_fifo_{in,out}_1</code>) and each backend instance
 * must claim an available pair exclusively at initialization time.
 *
 * <h3>How it works</h3>
 *
 * <ol>
 *   <li>{@link #claim(String)} tries indices 0..MAX-1, attempting to create <code>
 *       psl_fifo_{i}.lease</code> with <code>CREATE_NEW</code>. If the file already exists, that
 *       slot is considered taken and the next index is tried.
 *   <li>On success, the allocator verifies the corresponding FIFO paths exist and are FIFOs
 *       (S_IFIFO), then exposes the concrete <code>fifoIn</code> and <code>fifoOut</code> paths.
 *   <li>{@link #openClientIO()} opens the FIFOs in a safe order for the client side (read
 *       <em>first</em> on the reply/out FIFO, then write on the request/in FIFO) to avoid blocking
 *       on open.
 *   <li>{@link #close()} releases the lease by deleting the <code>.lease</code> file.
 * </ol>
 *
 * <h3>Thread-safety</h3>
 *
 * <p>Instances are not intended for concurrent mutation beyond construction and closing. Use one
 * allocator per backend instance.
 *
 * <h3>Crash/stale leases</h3>
 *
 * <p>If the process crashes, the OS closes FIFO descriptors but the lease file may remain. You can
 * store the PID in the lease file and reclaim stale leases by checking {@link
 * java.lang.ProcessHandle#of(long)} before deletion.
 *
 * <h3>Preconditions</h3>
 *
 * <ul>
 *   <li>The base directory contains pre-created FIFOs named <code>psl_fifo_{i}_in</code> and <code>
 *       psl_fifo_{i}_out</code> for 0..MAX-1. (Create with <code>mkfifo</code>.)
 *   <li>All cooperating processes respect the lease protocol (no direct FIFO use without a lease).
 * </ul>
 *
 * <h3>Example</h3>
 *
 * <pre>{@code
 * try (FifoLeaseAllocator lease = FifoLeaseAllocator.claim("/tmp")) {
 *   System.out.println("Using FIFO index with in=" + lease.fifoIn + " out=" + lease.fifoOut);
 *   try (FifoLeaseAllocator.FifoIO io = lease.openClientIO()) {
 *     io.inWriter.write("W tag key value\n");
 *     io.inWriter.flush();
 *     String line = io.outReader.readLine();
 *     // handle reply...
 *   }
 * } // lease released (lease file deleted)
 * }</pre>
 *
 * <h3>Errors</h3>
 *
 * <ul>
 *   <li>{@link IllegalStateException} if no free slot is available.
 *   <li>{@link java.nio.file.NoSuchFileException} if expected FIFOs are missing.
 *   <li>{@link IllegalArgumentException} if a path exists but is not a FIFO.
 * </ul>
 *
 * @see java.nio.file.Files
 * @see java.nio.file.StandardOpenOption#CREATE_NEW
 */
public final class FifoLeaseAllocator implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FifoLeaseAllocator.class);
    public static final int MAX = 4; // you said “there’s only 2 at most”
    private final int index; // 0 or 1
    private final Path leasePath;
    public final String fifoIn; // claimed fifo paths
    public final String fifoOut;

    private FifoLeaseAllocator(int idx, Path leasePath, String in, String out) {
        this.index = idx;
        this.leasePath = leasePath;
        this.fifoIn = in;
        this.fifoOut = out;
    }

    public static long currentPid() {
        // Works on Java 8+
        try {
            String jvmName = ManagementFactory.getRuntimeMXBean().getName(); // e.g., "12345@host"
            int i = jvmName.indexOf('@');
            if (i > 0) {
                return Long.parseLong(jvmName.substring(0, i));
            }
        } catch (Throwable ignore) {
        }
        return -1L;
    }

    /** Try to claim any free FIFO in [0, MAX). Throws if none available. */
    public static FifoLeaseAllocator claim(String baseDir) throws Exception {
        Objects.requireNonNull(baseDir, "baseDir");
        Path dir = Paths.get(baseDir);
        Files.createDirectories(dir);

        // Try indices in order; you can randomize or round-robin if you like
        for (int i = 1; i <= MAX; i++) {
            Path lease = dir.resolve("psl_fifo_" + i + ".lease");
            try {
                Files.write(
                        lease,
                        ("pid=" + currentPid() + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);

                // Success: verify FIFOs exist (bootstrap should mkfifo these)
                String in = dir.resolve("psl_fifo_in" + i).toString();
                String out = dir.resolve("psl_fifo_out" + i).toString();
                LOG.info("Leased FIFO index: " + i + " with in=" + in + " out=" + out);
                ensureIsFifo(in);
                ensureIsFifo(out);
                return new FifoLeaseAllocator(i, lease, in, out);

            } catch (FileAlreadyExistsException taken) {
                // someone else holds this index; try next
            }
        }
        throw new IllegalStateException(
                "No free FIFO slots in " + baseDir + " (need ≤ " + MAX + ")");
    }

    /** Open order for client side: open OUT (read) first, then IN (write). */
    public FifoIO openClientIO() throws Exception {
        BufferedReader outReader = null;
        // new BufferedReader(
        //         new InputStreamReader(
        //                 new FileInputStream(fifoOut), StandardCharsets.UTF_8));
        BufferedWriter inWriter =
                new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(fifoIn), StandardCharsets.UTF_8));
        return new FifoIO(inWriter, outReader);
    }

    private static void ensureIsFifo(String path) throws Exception {
        Path p = Paths.get(path);
        if (!Files.exists(p)) {
            throw new NoSuchFileException("Missing FIFO: " + path);
        }
        int mode = (Integer) Files.getAttribute(p, "unix:mode");
        boolean isFifo = (mode & 0xF000) == 0x1000; // S_IFIFO
        if (!isFifo) {
            throw new IllegalArgumentException("Not a FIFO: " + path);
        }
    }

    @Override
    public void close() {
        try {
            Files.deleteIfExists(leasePath);
        } catch (Exception ignore) {
        }
    }

    /** Simple holder for opened streams (close them in your backend’s dispose/close). */
    public static final class FifoIO implements AutoCloseable {
        public final BufferedWriter inWriter; // client -> server
        public final BufferedReader outReader; // server -> client

        FifoIO(BufferedWriter inWriter, BufferedReader outReader) {
            this.inWriter = inWriter;
            this.outReader = outReader;
        }

        @Override
        public void close() {
            try {
                inWriter.close();
            } catch (Exception ignore) {
            }
            try {
                outReader.close();
            } catch (Exception ignore) {
            }
        }
    }
}
