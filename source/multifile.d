module multifile;

import std.file : exists;
import std.string : toStringz;
import std.container.rbtree;

//import core.stdc.stdio;
import core.sys.posix.sys.mman;
import core.sys.posix.stdio;
import core.sys.posix.unistd;

/**
  A Multifile is a file containing multiple independently modifiable data chunks.

  Each data chunk can grow. Growing a data chunk may cause it to be reallocated.

  Multifile is designed to handle small numbers of chunks (around 100k). It reads its index into
  memory, which may not be appropriate for some use cases.

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
      */
    this(string file, bool createIfNecessary)
    {
        if (!exists(file))
        {
            if (createIfNecessary)
            {
                fd = fopen(file.toStringz, "a+");
                fd.write(MAGIC);
                fd.writeUlong(DATA_START);
                fd.writeUlong(0);
            }
            else
            {
                throw new Exception("Softie multifile " ~ file ~ " does not exist");
            }
        }
        else
        {
            fd = fopen(file.toStringz, "a+");
        }
        filenumber = fileno(fd);
        fseek(fd, 0, SEEK_SET);
        // verify MAGIC
        ubyte[4] m;
        fd.read(m[]);
        if (m != MAGIC)
        {
            throw new Exception(
                    "Incorrect magic bytes in file " ~
                    file ~ ". Is this a softie multifile?");
        }

        // find where the index is
        auto indexPos = fd.readUlong;
        auto indexFd = fopen(file.toStringz, "w+");
        fseek(indexFd, indexPos, SEEK_SET);
        index = Index(indexFd);
        index.read();
    }

    /**
        Manipulate the subfile with the given name.

        Require that it be at least minLength bytes long. If it is shorter, it will be extended in
        place or reallocated.
      */
    void manipulate(string name, ulong minLength, void delegate(scope ubyte[]) dg)
    {
        auto entry = fetchWithLength(name, minLength);
        auto ptr = mmap(
                null,
                entry.length,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                filenumber,
                entry.start);
        scope(exit)munmap(ptr, entry.length);
        auto data = (cast(ubyte*)ptr)[0..entry.length];
        dg(data);
    }

    void write(string name, ulong offset, ubyte[] data)
    {
        auto entry = fetchWithLength(name, offset + data.length);
        fseek(fd, entry.start + offset, SEEK_SET);
        fd.write(data);
    }

private:
    int filenumber;
    FILE* fd;
    Index index;

    /*
       Fetch an entry with the given name
     */
    Entry fetchWithLength(string name, ulong minLength)
    {
        auto entry = index.get(name);
        if (!entry.exists)
        {
            return index.create(name, minLength);
        }
        if (entry.length >= minLength)
        {
            return entry;
        }
        if (index.resizeInPlace(entry, minLength))
        {
            return entry;
        }
        auto next = index.create("$$softie-tmp-resize", minLength);
        auto src = fdopen(dup(fileno(fd)), "a+");
        scope (exit) fclose(src);
        fseek(src, entry.start, SEEK_SET);
        fseek(fd, next.start, SEEK_SET);
        ubyte[1] buf;
        for (ulong i = 0; i < minLength; i++)
        {
            src.read(buf);
            fd.write(buf);
        }
        index.remove(entry);
        index.rename(next, name);
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

    RedBlackTree!(Entry, "a.start < b.start") byPosition;
    RedBlackTree!(Entry, "a.name < b.name") byName;

    this(FILE* fd)
    {
        this.fd = fd;
    }

    /// Read an index from the file at its current location.
    void read()
    {
        auto len = fd.readUlong;
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
                remove(e);
                writeToNewSection();
            }
            else
            {
                fseek(fd, r.front.start, SEEK_SET);
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
        auto reserved = size + entry.headerSize;
        entry.length = reserved;
        auto gap = findGap(reserved);
        entry.start = gap;
        insert(entry);
        fseek(fd, gap, SEEK_SET);
        writeHere();
    }

    void writeHere()
    {
        auto start = ftell(fd);
        fd.writeUlong(byName.length);
        foreach (entry; byName)
        {
            entry.write(fd);
        }
        fseek(fd, INDEX_POINTER_POSITION, SEEK_SET);
        fd.writeUlong(cast(ulong)start);
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
        return name.length + 4 * ulong.sizeof;
    }

    void write(FILE* fd)
    {
        fd.writeUlong(start);
        fd.writeUlong(length);
        fd.writeUlong(name.length);
        fd.write(cast(const(ubyte[]))name);
        fd.writeUlong(0);
    }

    static Entry read(FILE* fd)
    {
        Entry e;
        e.start = fd.readUlong;
        e.length = fd.readUlong;
        e.name = fd.readLenPrefixString;
        auto barrier = fd.readUlong;
        if (barrier != 0)
        {
            throw new Exception("Multifile index is corrupted");
        }
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
    if (fread(buf.ptr, ubyte.sizeof, buf.length, fd) != buf.length)
    {
        throw new Exception("insufficient read");
    }
}

void write(FILE* fd, scope const ubyte[] buf)
{
    if (fwrite(buf.ptr, ubyte.sizeof, buf.length, fd) != buf.length)
    {
        throw new Exception("failed to write to file");
    }
}
