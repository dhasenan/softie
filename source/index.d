module softie.index;

import softie.core;
import softie.util;

import std.stdio : File;
import std.file : exists;
import std.string : toStringz;
import std.container.rbtree;

import cstdio = core.stdc.stdio;

import multifile;


/**
    A simple, synchronous full text search index.
*/
class Index : WordWriter
{
    /**
    Create a new Index.

    Params:
        filename = the name of the file to store data in
    */
    this(string filename, ulong estimatedFiles = 10_000)
    {
        this.filename = filename;
        this.file = Multifile(filename, true);
        auto docBytes = file.read(DOCINDEX_NAME);
        if (docBytes == null)
        {
            // allocate some default space
            // estimate ~20 character names, 16 bytes overhead (length plus ID)
            file.write(DOCINDEX_NAME, estimatedFiles * 36, null);
        }
        nextDocEntryOffset = docBytes.length;
        while (docBytes != null)
        {
            // id
            // length
            // bytes
            ulong id = 0;
            foreach (i; 0..8)
            {
                id <<= 8;
                id |= docBytes[0];
                docBytes = docBytes[1..$];
            }
            if (id == 0)
            {
                // We're exiting early but preallocated some space.
                nextDocEntryOffset = nextDocEntryOffset - docBytes.length - 8;
            }
            ulong length = 0;
            foreach (i; 0..8)
            {
                length <<= 8;
                length |= docBytes[0];
                docBytes = docBytes[1..$];
            }
            auto front = docBytes[0..length];
            docBytes = docBytes[length..$];
            auto name = cast(string)front;
            auto doc = docid(id);
            byName[name] = doc;
            byId[doc] = name;
        }
    }

    /**
        Add a reference to `word` at the given location in the given document.

        Words must not begin with a dollar sign.
    */
    void insert(string word, docid document, ulong offset)
    in
    {
        assert(word[0] != '$');
    }
    body
    {
        ubyte[16] buf;
        ubyte[] b = buf[];
        pushUlong(cast(ulong)document, b);
        pushUlong(offset, b);
        file.append(DOCINDEX_NAME, buf);
    }

    /**
        Locate references to the given word.

        Params:
            word = the word to search for
            count = the maximum number of records to return
            offset = number of records to skip
    */
    Ref[] matches(string word, ulong offset = 0, ulong count = 100)
    {
        auto b = file.read(word, Ref.DISK_SIZE * offset, Ref.DISK_SIZE * count);
        auto r = new Ref[b.length / Ref.DISK_SIZE];
        foreach (i; 0..r.length)
        {
            r[i] = Ref.pop(b);
        }
        return r;
    }

    /**
        Locate references to the given word.

        Params:
            word = the word to search for
            offset = number of records to skip
            buffer = the buffer to fill with data
        Returns: the buffer you passed in
    */
    Ref[] matches(string word, Ref[] buffer, ulong offset = 0)
    {
        auto b = file.read(word, Ref.DISK_SIZE * offset, Ref.DISK_SIZE * buffer.length);
        foreach (i; 0..buffer.length)
        {
            buffer[i] = Ref.pop(b);
        }
        return buffer;
    }


    /**
        Locate or create a document id for the given document name.
    */
    docid documentId(string documentName)
    {
        if (auto id = documentName in byName)
        {
            return *id;
        }
        auto d = docid(byId.length + 1);
        byId[d] = documentName;
        byName[documentName] = d;
        auto buf = new ubyte[16 + documentName.length];
        auto b2 = buf;
        pushUlong(cast(ulong)d, b2);
        pushUlong(documentName.length, b2);
        b2[] = cast(const(ubyte)[])documentName;
        file.write(DOCINDEX_NAME, nextDocEntryOffset, buf);
        nextDocEntryOffset += buf.length;
        return d;
    }

    /**
        Locate the name of the given document id.

        Returns:
            the document name, or null if the document wasn't found
    */
    string documentName(docid id)
    {
        if (auto p = id in byId)
        {
            return *p;
        }
        return null;
    }

    /// Maximum length for a document name.
    enum MAX_NAME_LENGTH = 256;

private:
    enum META_NAME = "$META";
    enum DOCINDEX_NAME = "$DOCS";

    string filename;
    Multifile file;
    string[docid] byId;
    docid[string] byName;
    ulong nextDocEntryOffset;
}

class SoftieException : Exception
{
    this(string msg) { super(msg); }
}

private:

struct Word
{
    // rbtree doesn't work with members with const fields
    string text;
    ulong start;
    ulong count;

    int opCmp(ref const Word other) const pure nothrow @nogc
    {
        import std.algorithm.comparison : cmp;
        return cmp(text, other.text);
    }
}

struct Ref
{
    enum DISK_SIZE = 16;
    docid document;
    ulong offset;

private:
    void write(ubyte[DISK_SIZE] buf)
    {
        auto b = buf[];
        pushUlong(cast(ulong)document, b);
        pushUlong(offset, b);
    }

    static Ref pop(ref ubyte[] buf)
    {
        auto b = buf;
        Ref r;
        r.document = docid(b.popUlong);
        r.offset = b.popUlong;
        buf = b;
        return r;
    }
}
