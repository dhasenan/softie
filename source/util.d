module softie.util;

void toBuf(ulong v, ubyte[] buf)
in
{
    assert(buf.length >= 8);
}
body
{
    for (int i = 7; i >= 0; i--)
    {
        buf[i] = cast(ubyte)(v & 0xFF);
        v >>= 8;
    }
}

void pushUlong(ulong v, ref ubyte[] buf)
{
    v.toBuf(buf);
    buf = buf[8..$];
}

ulong popUlong(ref ubyte[] b)
in
{
    assert(b.length >= 8);
}
body
{
    ulong u;
    for (int i = 0; i < 8; i++)
    {
        u <<= 8;
        u |= b[i];
    }
    b = b[8..$];
    return u;
}
