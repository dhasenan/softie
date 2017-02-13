module multifile;

import std.container.rbtree;
import std.file : exists;
import std.format;
import std.string : toStringz, fromStringz;

import std.experimental.logger;

import core.stdc.errno;
import core.stdc.string;
import core.sys.posix.stdio;
import core.sys.posix.sys.mman;
import core.sys.posix.unistd;

version(unittest) version = multifilelog;

/**
  A Multifile is a file containing multiple independently modifiable data chunks.

  Each data chunk can grow. Growing a data chunk may cause it to be reallocated.

  Multifile is designed to handle small numbers of chunks (around 100k). It reads its index into
  memory, which may not be appropriate for some use cases. The remainder of the file is only read as
  necessary.

  A Multifile does not give you extra padding so that your files can grow in place. If you want
  padding, please manage that on your own. In essence, Multifile is optimized more for compactness
  than for quick appending.

  Entries in a Multifile prefixed with "$$" are reserved for internal use.
 */
struct Multifile
{
    enum ubyte[4] MAGIC = [0x53, 0x6f, 0x66, 0x2b];  // "Sof+"
    enum DATA_START = 12;

    /**
      Open a Multifile, creating it if necessary.

      Params:
        file = The path to the file.
        createIfNecessary = Whether to create the file if necessary. Does not create directories.
    */
    this(string file, bool createIfNecessary)
    {
        this.filename = file;
        this.fileZ = file.toStringz;
        if (!exists(file))
        {
            if (createIfNecessary)
            {
                version(multifilelog) infof("creating file %s", file);
                fd = fopen(fileZ, "w+");
                fd.write(MAGIC);
                fd.writeUlong(DATA_START);
                fd.writeUlong(0);
                fd.fflush();
            }
            else
            {
                throw new Exception("Softie multifile " ~ file ~ " does not exist");
            }
        }
        else
        {
            version(multifilelog) infof("using existing file %s", file);
            fd = fopen(fileZ, "r+");
        }
        filenumber = fileno(fd);
        version(multifilelog) infof("filenumber: %s", filenumber);
        seek(fd, 0);
        // verify MAGIC
        ubyte[4] m;
        fd.read(m[]);
        if (m != MAGIC)
        {
            throw new Exception(
                    "Incorrect magic bytes in file " ~
                    file ~ ". Is this a softie multifile?");
        }

        version(multifilelog) infof("verified magic");

        // find where the index is
        auto indexPos = fd.readUlong;
        version(multifilelog) infof("index starts at %s", indexPos);

        // open and read the index
        auto indexFd = fopen(fileZ, "r+");
        seek(indexFd, indexPos);
        index = Index(indexFd);
        index.read();

        version(multifilelog) infof("read index");
    }

    /**
        Manipulate the subfile with the given name.

        Require that it be at least minLength bytes long. If it is shorter, it will be extended in
        place or reallocated.

        This will create it if it doesn't already exist. In this case, its contents are undefined.

        The buffer is memory mapped in.

        If the index must be rewritten (it's a new entry or we had to move the subfile), then this
        will write the index to the file. Otherwise, this does not cause any writes.

        The length of the underlying segment must be an integer number of pages.
      */
    void manipulate(string name, ulong minLength, void delegate(scope ubyte[]) dg)
    {
        checkNotClosed;
        auto entry = fetchWithLength(name, minLength);
        if (entry.length == 0)
        {
            throw new Exception("failed to reallocate entry");
        }
        version(multifilelog) infof("mmap: %s bytes at %s", entry.length, entry.start);
        auto ptr = mmap(
                null,
                entry.length,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                filenumber,
                entry.start);
        if (ptr == cast(void*)-1)
        {
            auto err = strerror(errno);
            throw new Exception("failed to mmap file: " ~ err.fromStringz.idup);
        }
        scope(exit)munmap(ptr, entry.length);
        auto data = (cast(ubyte*)ptr)[0..entry.length];
        dg(data);
    }

    /**
        Read the specified slice of the given entry.

        This will return as much data as available, up to count bytes. If less is available, you
        will get a smaller result than you asked for.
      */
    ubyte[] read(string name, ulong offset, ulong count)
    {
        auto entry = index.get(name);
        if (!entry.exists) return null;
        auto avail = entry.length - offset;
        if (avail > count) avail = count;
        auto buf = new ubyte[entry.length - offset];
        seek(fd, entry.start + offset);
        fd.read(buf);
        return buf;
    }

    /**
        Read the entirety of the given entry.
      */
    ubyte[] read(string name)
    {
        auto entry = index.get(name);
        if (!entry.exists) return null;
        auto buf = new ubyte[entry.length];
        seek(fd, entry.start);
        version(multifilelog) infof("reading %s bytes from entry %s at offset %s", buf.length, name, entry.start);
        fd.read(buf);
        return buf;
    }

    /**
        Write data directly to the given entry.
      */
    void write(string name, ulong offset, scope const ubyte[] data)
    {
        checkNotClosed;
        auto entry = fetchWithLength(name, offset + data.length);
        auto start = entry.start + offset;
        seek(fd, entry.start + offset);
        version(multifilelog) infof("writing %s bytes to entry %s at offset %s", data.length, name, ftell(fd));
        fd.write(data);
        fd.fflush();
        version(multifilelog) infof("wrote %s data bytes to file", data.length);
    }

    /**
        Flush data to the file.
      */
    void flush()
    {
        checkNotClosed;
        index.write();
        fflush(fd);
    }

    void close()
    {
        flush();
        fclose(fd);
        fd = null;
    }

private:
    string filename;
    string fileZ;
    int filenumber;
    FILE* fd;
    FILE* fd;
    Index index;

    void checkNotClosed()
    {
        if (fd is null)
        {
            throw new Exception("tried to perform an operation on a closed Multifile");
        }
    }

    /*
       Fetch an entry with the given name
     */
    Entry fetchWithLength(string name, ulong minLength)
    {
        auto entry = index.get(name);
        if (!entry.exists)
        {
            entry = index.create(name, minLength);
            index.write();
            return entry;
        }
        version(multifilelog) infof("entry %s exists already", name);
        version(multifilelog) infof("length: %s vs %s", entry.length, minLength);
        if (entry.length >= minLength)
        {
            return entry;
        }
        if (index.resizeInPlace(entry, minLength))
        {
            version(multifilelog) infof("resized in place");
            return entry;
        }
        version(multifilelog) infof("manually moving and resizing");
        auto next = index.create("$$softie-tmp-resize", minLength);
        auto src = fopen(fileZ, "r+");
        version(multifilelog) infof("read fd created");
        scope (exit) fclose(src);
        version(multifilelog) infof("copying data from %s to %s", entry.start, next.start);
        fseek(fd, 0, SEEK_END);
        auto end = ftell(fd);
        version(multifilelog) infof("last file position is %s", ftell(fd));
        seek(src, entry.start);
        if (end == next.start)
        {
            seekEnd(fd);
        }
        else
        {
            seek(fd, next.start);
        }
        ubyte[1] buf;
        for (ulong i = 0; i < entry.length; i++)
        {
            src.read(buf);
            fd.write(buf);
        }
        version(multifilelog) infof("done copying; zeroing out the rest");
        buf[0] = 0;
        for (ulong i = entry.length; i < minLength; i++)
        {
            fd.write(buf);
        }
        version(multifilelog) infof("zeroed out remainder; updating index");
        index.remove(entry);
        index.rename(next, name);
        flush();
        return next;
    }
}

/*

The format of a Multifile:

ulong is 8 bytes little-endian

 * MAGIC (4 octets)
 * Location of range index (ulong, offset from start of file)
 * Series of ranges and gaps

A range is an unmarked series of bytes.

A gap is unused area within the file.

The range index is:
 * Number of entries (ulong)
 * Entries

An entry is:
 * File offset
 * Length of range
 * Length of name (ulong)
 * Name bytes
 * Barrier (ulong 0)

The range index should always contain one entry: itself.
*/
struct Index
{
    /// The name of the index range.
    enum INDEX_NAME = "$$softie-index$$";

    // immediately after magic bytes
    enum INDEX_POINTER_POSITION = 4;

    /// Number of bytes for the on-disk range corresponding to the index.
    ulong size = ulong.sizeof;

    FILE* fd;

    alias RedBlackTree!(Entry, "a.start < b.start") ByPosition;
    alias RedBlackTree!(Entry, "a.name < b.name") ByName;
    ByPosition byPosition;
    ByName byName;

    this(FILE* fd)
    {
        this.fd = fd;
        byPosition = new ByPosition;
        byName = new ByName;
    }

    /// Read an index from the file at its current location.
    void read()
    {
        auto len = fd.readUlong;
        version(multifilelog) infof("index has %s entries", len);
        for (int i = 0; i < len; i++)
        {
            insert(Entry.read(fd));
        }
    }

    /// Write the index to the file.
    void write()
    {
        auto r = byName.equalRange(Entry(INDEX_NAME, 0, 0));
        if (r.empty)
        {
            writeToNewSection();
        }
        else
        {
            auto e = r.front;
            auto next = byPosition.upperBound(e);
            if (!next.empty && next.front.start < e.start + size)
            {
                // We need to find another location for the index.
                // Remove it first so it doesn't mess us up later.
                remove(e);
                writeToNewSection();
            }
            else
            {
                seek(fd, r.front.start);
                writeHere();
            }
        }
    }

    Entry get(string name)
    {
        auto check = Entry(name, 0, 0);
        auto r = byName.equalRange(check);
        if (r.empty) return check;
        return r.front;
    }

    /// Insert a new entry.
    void insert(Entry entry)
    {
        byPosition.insert(entry);
        byName.insert(entry);
        size += entry.headerSize;
    }

    /// Remove an existing entry.
    void remove(Entry entry)
    {
        byPosition.removeKey(entry);
        byName.removeKey(entry);
        size -= entry.headerSize;
    }

    bool resizeInPlace(ref Entry entry, ulong newLength)
    {
        auto r = byPosition.upperBound(entry);
        if (!r.empty && r.front.start < entry.start + newLength)
        {
            return false;
        }
        remove(entry);
        entry.length = newLength;
        insert(entry);
        return true;
    }

    void rename(ref Entry entry, string newName)
    {
        remove(entry);
        entry.name = newName;
        insert(entry);
    }

    Entry create(string name, ulong length)
    {
        auto e = Entry(name, findGap(length), length);
        version(multifilelog) infof("created new entry %s", e);
        insert(e);
        return e;
    }

private:
    /*
        Find where in the file we can insert this many bytes.
        This does a linear scan.
      */
    ulong findGap(ulong length)
    {
        ulong last = DATA_START;
        foreach (e; byPosition)
        {
            if (last + length <= e.start)
            {
                return last;
            }
            last = e.end;
        }
        return last;
    }

    void writeToNewSection()
    {
        // The index hasn't indexed itself or needs to move.
        // We have to create the entry a bit manually because it's awkward to calculate the size
        // and position all in one go.
        auto entry = Entry(INDEX_NAME, 0, 0);

        // We don't have the index entry at this point.
        auto reserved = size + entry.headerSize;

        // Give us a margin so we don't have to grow quite as fast.
        reserved += reserved >> 1;
        entry.length = reserved;

        // Figure out where to put it...
        auto gap = findGap(reserved);

        // And put it there.
        entry.start = gap;
        insert(entry);
        seek(fd, gap);
        writeHere();
    }

    void writeHere()
    {
        auto start = ftell(fd);
        version(multifilelog) infof("writing index to position %s", start);
        fd.writeUlong(byName.length);
        version(multifilelog) infof("we have %s entries", byName.length);
        foreach (entry; byName)
        {
            entry.write(fd);
        }
        seek(fd, INDEX_POINTER_POSITION);
        fd.writeUlong(cast(ulong)start);
        version(multifilelog) infof("wrote index pointer %s", cast(ulong)start);
    }
}

enum DATA_START = 12;

struct Entry
{
    string name;
    ulong start;
    ulong length;

    bool exists()
    {
        return length > 0;
    }

    ulong end()
    {
        return start + length;
    }

    ulong headerSize()
    {
        return name.length + 3 * ulong.sizeof;
    }

    void write(FILE* fd)
    {
        fd.writeUlong(start);
        fd.writeUlong(length);
        fd.writeUlong(name.length);
        fd.write(cast(const(ubyte[]))name);
    }

    static Entry read(FILE* fd)
    {
        Entry e;
        e.start = fd.readUlong;
        e.length = fd.readUlong;
        e.name = fd.readLenPrefixString;
        return e;
    }
}

string readLenPrefixString(FILE* fd) @trusted
{
    import std.exception : assumeUnique;

    auto length = fd.readUlong;
    auto bytes = new ubyte[length];
    fd.read(bytes);
    return cast(string)bytes;
}

ulong readUlong(FILE* fd)
{
    ubyte[8] buf;
    fd.read(buf);
    ulong v = 0;
    foreach (b; buf)
    {
        v <<= 8;
        v |= b;
    }
    return v;
}

void writeUlong(FILE* fd, ulong v)
{
    ubyte[8] buf;
    for (int i = buf.length - 1; i >= 0; i--)
    {
        buf[i] = cast(ubyte)(v & 0xFF);
        v >>= 8;
    }
    fd.write(buf);
}

void read(FILE* fd, scope ubyte[] buf)
{
    auto count = fread(buf.ptr, ubyte.sizeof, buf.length, fd);
    if (count != buf.length)
    {
        auto c = ftell(fd);
        throw new Exception("insufficient read: got %s, expected %s, at offset %s".format(
            count, buf.length, c));
    }
}

void write(FILE* fd, scope const ubyte[] buf)
{
    if (fwrite(buf.ptr, ubyte.sizeof, buf.length, fd) != buf.length)
    {
        throw new Exception("failed to write to file");
    }
}

void seek(FILE* fd, ulong pos)
{
    auto res = fseek(fd, pos, SEEK_SET);
    if (res != 0)
    {
        auto err = errno;
        auto errstr = strerror(err).fromStringz.idup;
        throw new Exception("failed to seek to position %s in file: %s".format(pos, errstr));
    }
}

void seekEnd(FILE* fd)
{
    auto res = fseek(fd, 0, SEEK_END);
    if (res != 0)
    {
        auto err = errno;
        auto errstr = strerror(err).fromStringz.idup;
        throw new Exception("failed to seek to end of file file: %s".format(errstr));
    }
}


unittest
{
    import std.file;

    enum filename = "unittest.sfm";

    try
    {
        remove(filename);
    }
    catch (Exception e)
    {
        // is okay
    }

    auto multi = Multifile(filename, true);

    ubyte[] data = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55];
    multi.write("test-range-1", 0, data);
    version(multifilelog) infof("wrote data");
    auto read1 = multi.read("test-range-1");
    assert(read1 == data, "wanted %s got %s".format(data, read1));
    auto data2 = cast(const ubyte[])"A british tar is a soaring soul";
    multi.write("test-range-2", 0, data2);
    version(multifilelog) infof("wrote data2");
    assert(multi.read("test-range-2") == data2);
    multi.flush;
    multi.close;

    multi = Multifile(filename, false);
    read1 = multi.read("test-range-1");
    assert(read1 == data, "wanted %s got %s".format(data, read1));
    assert(multi.read("test-range-2") == data2);
    auto data2part2 = cast(const ubyte[]) " as free as a mountain bird";
    multi.write("test-range-2", data2.length, data2part2);
    multi.write("test-range-1", data.length - 2, [3, 1, 4, 1, 5, 9]);
    multi.close;


    multi = Multifile(filename, false);
    assert(
            (cast(string)multi.read("test-range-2").idup) ==
            "A british tar is a soaring soul as free as a mountain bird");
    ubyte[] expected = [
        1, 1, 2, 3, 5, 8, 13, 21,
        3, 1, 4, 1, 5, 9
    ];
    assert(multi.read("test-range-1") == expected);

}
