module softie.index;

import softie.core;

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
        stopWords = words to avoid indexing (see softie.stopwords.$LANG)
        wordSize = how long words are. Words longer than this are truncated, resulting in ambiguity.
        numWordHint = how many words you expect to insert into the index.
        numDocumentsHint = how many documents you expect to refer to in the index.
    */
    this(
        string filename,
        bool[string] stopWords = null,
        ubyte wordSize = 14,
        ulong numWordHint = 50_000,
        ulong numDocumentsHint = 50_000)
    {
        import std.algorithm.comparison : max;

        this.filename = filename;
        this.stopWords = stopWords;
        this.wordSize = max(wordSize, cast(ubyte)8);
        this.numWordHint = max(numWordHint, 5_000);
        this.numDocumentsHint = max(numDocumentsHint, 500);

        auto wordStructLength = wordSize + 2*ulong.sizeof;
        auto wordReservation = wordStructLength *  numWordHint;
        // Extra space, just in case.
        wordReservation += wordReservation >> 1;
        // Figure out the start of the files section.
        filesOffset = wordsOffset + wordReservation;
        auto b = filesOffset & (PAGE_SIZE - 1);
        if (b != 0)
        {
            filesOffset += PAGE_SIZE;
            filesOffset -= b;
        }

        if (filename.exists)
        {
            this.open();
        }
        else
        {
            this.writeInitialData();
        }
    }

    /**
        Add a reference to `word` at the given location in the given document.
    */
    void insert(string word, docid document, ulong offset)
    {

    }

    /**
        Locate or create a document id for the given document name.

        Document names can be up to 256 characters.
    */
    docid documentId(string documentName)
    {
    }

    /// Maximum length for a document name.
    enum MAX_NAME_LENGTH = 256;

private:
    string filename;

    void open()
    {
        db = Database(filename);
    }
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
    size_t start;
    size_t count;

    int opCmp(ref const Word other) const pure nothrow @nogc
    {
        import std.algorithm.comparison : cmp;
        return cmp(text, other.text);
    }
}

