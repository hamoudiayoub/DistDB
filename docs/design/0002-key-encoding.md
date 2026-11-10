# 0002 — Order-preserving key encoding

> Draft written while building Phase 1; rewrite it in your own words.

## Problem

The storage engine, ranges and scans order keys as unsigned bytes. SQL needs keys whose byte
order equals the order of the typed values inside them (`/t/{table}/pk/{id}`, index entries with
several columns, some descending), so that a range scan over bytes is a range scan over values.

## Format (`io.distdb.common.encoding`)

A key is a sequence of components. Each component is a tag byte followed by its payload:

| Type    | Tag      | Payload                                                         |
|---------|----------|-----------------------------------------------------------------|
| NULL    | `05`     | none                                                            |
| BOOLEAN | `10`/`11`| none (false/true)                                               |
| LONG    | `20`     | 8 bytes big-endian, sign bit flipped                            |
| DOUBLE  | `30`     | 8 bytes; positives flip the sign bit, negatives invert all bits |
| BYTES   | `40`     | content with `00` escaped as `00 FF`, ended by `00 01`          |
| STRING  | `41`     | UTF-8, escaped and terminated like BYTES                        |

**Descending** is the ascending encoding of that component with every byte inverted, tag
included. Ascending tags are below `0x80` and descending tags are above `0x80`, so each component
says what its type and direction are, and `KeyReader.toDebugString` can render any key without a
schema (for example `/7/"accounts"/NULL(desc)`).

## Why it is correct

- Fixed-width big-endian with the sign flipped turns signed comparison into unsigned comparison.
- Escaping makes `00` inside content sort as `00 FF`, above the terminator `00 01`. So a value
  that is a prefix of another sorts first, and no content byte is mistaken for the end.
- Every component encoding is **prefix-free**. For prefix-free codes the first differing byte
  decides the comparison, so inverting the bytes reverses the order exactly. That is why
  descending is just byte inversion.
- A key that is a prefix of another (fewer components) sorts first, because `Bytes.compare`
  orders shorter prefixes first.

## Semantics to remember

- Doubles order like `Double.compare`: `-0.0 < 0.0`, and NaN is largest and canonicalized. SQL
  treats `-0.0 = 0.0`, so the SQL layer must normalize before encoding.
- Strings order by **code point** (PostgreSQL `C` collation), not by `String.compareTo`, which
  compares UTF-16 units. Unpaired surrogates are rejected.
- NULL sorts first ascending and last descending.
- Tags are an on-disk format. `KeyEncodingTest.goldenBytes` pins them.

## Verification (the Phase 1 gate)

`KeyEncodingPropertiesTest` (jqwik) checks, for each type in both directions, that the order of
the bytes equals the order of the values, that values round-trip, and that the encoding is
prefix-free. It then checks the same for random composite schemas with mixed directions, NULLs and
prefixes. The generators are biased towards `00`, `01`, `FE`, `FF`, every double bit pattern and
supplementary code points. A coverage check makes sure equal prefixes really occur.

Mutation check done during setup: disabling zero-escaping, or using the naive sign-flip for
negative doubles, makes the properties fail.

## Alternatives rejected

- **Varint ordered longs** (CockroachDB style): smaller, but more code. Fixed width is fine until
  benchmarks say otherwise, and the tag space leaves room for a compact variant later.
- **Length-prefixed strings**: these break ordering, because the length would be compared first.
