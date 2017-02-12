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
        bool[string] stopWords = null)
    {
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
        return docid.init;
    }

    /// Maximum length for a document name.
    enum MAX_NAME_LENGTH = 256;

private:
    string filename;

    void open()
    {
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

