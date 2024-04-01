# Provide Unique IDs and Metadata for Your Files

If you're reading this, you probably belong to an organization that maintains a
database containing many files of different sorts, and you probably want to make
it easier for researchers to access (and cite!) the data in these files.

In order for a file to be made available to users of BER's Data Transfer System
(DTS), it must have a unique identifier by which users can refer to it. The
file's name is probably not unique, so it's not an appropriate identifier.
Unique file identifiers are such a basic need that your organization probably
already has its own set.

Any string containing a unique sequence of characters can serve as a unique file
identifier for the DTS. For example, the following identifiers are all
technically valid:

* `615a383dcc4ff44f36ca5ba2`
* `machine-generated-id-21242452`
* `icanseemyhousefromhere`

The important thing is that the file can be uniquely identified. Users won't be
typing these identifiers in manually--they'll typically obtain them by searching
your database and selecting desired results.

The DTS assigns a database specific prefix to your identifier that gives it a
_namespace_ inside which it is unique. For example, the JGI Data Portal
identifier `615a383dcc4ff44f36ca5ba2` is made available by the DTS as
`JDP:615a383dcc4ff44f36ca5ba2`.

Sometimes a database consists of more than one dataset, and each dataset has its
own identifiers. For example, [Uniprot](https://www.uniprot.org/), one of
the world's largest collections of protein sequences, defines unique identifiers
for each of several distinct datasets. [This link](https://www.uniprot.org/help/linking_to_uniprot)
illustrates some examples of these dataset-specific identifiers.

Because Uniprot's identifiers indicate which dataset they're from, all of the
identifiers could be used within a single namespace by the DTS. This is one way
of providing access to multiple datasets. The DTS team is happy to discuss
alternatives with you.

## File Metadata

Whatever file identification scheme your organization uses, you can provide the
DTS with the file information it needs by creating an endpoint that returns
file metadata for a given unique file identifier.

What's "metadata"? There is no universal answer to this question, of course. The
DTS is part of an effort to harmonize how our community searches for, obtains,
and cites scientific data, but the size and diversity of this community make it
difficult to find an answer even for our purposes alone!

Nevertheless, we press on. The metadata specification used by the DTS is likely
to undergo changes of all sorts as we figure things out, but we'll give our best
effort to updating documentation as these changes occur.

### DTS Metadata Specification (2024-04-01)

**Put something here**

## Endpoint Recommendations

### Example

### Existing implementations

