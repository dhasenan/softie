module softie.core;

import std.typecons : Typedef;

alias docid = Typedef!(ulong, 0, "softie.docid");

/**
    A base interface for indexes for writing.
*/
interface WordWriter
{
    /**
        Get or create an ID for the given document name (eg filepath).
    */
    docid documentId(string documentName);

    /**
        Add a reference to `word` at the given location in the given document.
    */
    void insert(string word, docid document, ulong offset);
}

/**
    A DocumentProcessor turns documents into index writes.

    The base functionality in a DocumentProcessor is tokenizing and stopwords, but you might have one that applies arbitrary transformations.

    This is an interface instead of a delegate for convenience of configuration for implementations. (Using a delegate won't save you allocations if it has any state.)
*/
abstract class DocumentProcessor
{
    bool[string] stopwords;

    this(WordWriter writer, bool[string] stopwords = null)
    {
        this.writer = writer;
        this.stopwords = stopwords;
    }

    void process(string name, string text)
    {
        auto documentId = writer.documentId(name);

    }

    /**
        Check if a given word is a stopword (should be omitted).

        This is protected so that implementations can override it with their own logic.
        By default, it just checks whether the word is in the stopwords collection.
    */
    protected bool isStopword(string word)
    {
        return !!(word in stopwords);
    }

    /// Add the given word at this offset.
    protected void write(string word, size_t offset)
    {
        if (!isStopword(word))
        {
            writer.insert(word, documentId, offset);
        }
    }

    protected abstract void process(string text);

    private WordWriter writer;
    private docid documentId;
}
