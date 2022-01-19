# An ORC reader for Rust

[![Rust build status](https://img.shields.io/github/workflow/status/travisbrown/orcrs/rust-ci.svg?label=rust)](https://github.com/travisbrown/orcrs/actions)
[![Java build status](https://img.shields.io/github/workflow/status/travisbrown/orcrs/java-ci.svg?label=java)](https://github.com/travisbrown/orcrs/actions)
[![Coverage status](https://img.shields.io/codecov/c/github/travisbrown/orcrs/main.svg)](https://codecov.io/github/travisbrown/orcrs)

This project contains tools for working with [Apache ORC][apache-orc] files from the [Rust programming language][rust].

ORC is an open source data format that lets you represent tables of data efficiently
(think [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), but with types, compression, indexing, etc.).

Please note that this software is **not** "open source",
but the source is available for use and modification by individuals, non-profit organizations, and worker-owned businesses
(see the [license section](#license) below for details).

## Example use case

I've recently been working with the [Twitter Stream Grab][twitter-stream-grab], a data set published by
the [Archive Team][archive-team] and the [Internet Archive][internet-archive] that includes billions of
tweets and Twitter user profiles collected between 2011 and 2021.

The Twitter Stream Grab is 5.2 terabytes of compressed JSON data, and around 50 terabytes uncompressed.
It takes many hundreds of hours of computing time to parse this data, which makes repeated processing impractical
for personal projects, or for projects by activist groups with limited resources.

Storing this much data can also be impractical. I personally spent several hundred dollars just getting a copy
from the Internet Archive's servers to Berlin, and storing a (compressed) copy in [S3][s3] currently costs
about $122 per month.

There are many kinds of derived data sets and products you might want to build from data like the Twitter
Stream Grab. One example is this [collection of several million Twitter user profile snapshots][stop-the-steal]
for accounts that were active in spreading false claims about voter fraud in 2020. I'm also running a
[web service][memory-lol] that allows users to look up past screen names for Twitter accounts.

I'm using the ORC format to make building projects like these from this data more practical.
The basic idea is that instead of re-processing the entire 50 terabytes of JSON data for each application,
you parse it once to extract the user profiles (and other information) into a set of ORC tables.

This intermediate representation is slightly more compact: for example the original compressed data for December 2020
takes up about 60 gigabytes, but the ORC table I've built for data from that month only
takes up about 21 gigabytes. This means storing the ORC representation of the full 10 years of data
only costs around $40 per month using a service like S3, but more importantly it means that it's much,
much cheaper and easier to process or query the data.

AWS's [Athena][athena] lets you run SQL queries directly against ORC files stored in S3, for example.
You can also use Athena to process CSV files in S3, but running _any_ SQL query against compressed CSV
files for the entire Twitter Stream Grab would cost at least $0.50 (since all of the two or three terabytes
of compressed data have to be scanned), while querying ORC in Athena generally costs a tiny fraction of that,
since the ORC format makes it possible to avoid scanning data that isn't relevant to the query.

Products like Athena are useful for exploring data like the Twitter Stream Grab,
and ORC makes this practical in terms of cost and time,
but it's also possible to process the ORC files directly, so that for example instead of spending hundreds
of hours of computing time to build a relational database of Twitter user info from the raw JSON data,
you can spend a few hours and extract the data from the ORC files.

## Why this project?

The ORC format was developed to be a native storage format for [Apache Hive][hive], which is
built on [Hadoop][hadoop], which is firmly in the Java ecosystem. I personally find Hive to be
extremely annoying and painful to work with, and I don't prefer writing Java.

There is also a [C++ API for ORC][orc-cpp], but I have a fair amount of related tooling already written in Rust,
and I wanted to learn more about the internals of the ORC spec, so I decided to try to put together this
implementation, and it only took a couple of days.

## Use

The project currently provides one command-line tool that does a couple of things:

```
$ target/release/orcrs --help
orcrs 0.1.0
Travis Brown <travisrobertbrown@gmail.com>

USAGE:
    orcrs [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -h, --help       Print help information
    -v, --verbose    Level of verbosity
    -V, --version    Print version information

SUBCOMMANDS:
    export    Export the contents of the ORC file
    help      Print this message or the help of the given subcommand(s)
    info      Dump raw info about the ORC file
```

To list all profiles for verified Twitter accounts from the provided sample data, for example:

```bash
target/release/orcrs -vvv export --header --columns 0,3,9 examples/ts-10k-2020-09-20.orc | egrep -v "(false|,)$"
id,screen_name,verified
561595762,morinaga_pino,true
1746230882607849472,weareoneEXO,true
29363584,Sandi,true
2067989391190130694,WayV_official,true
36764368,AdamParkhomenko,true
53970806,stephengrovesjr,true
15327404,fox32news,true
1678598579585548288,Mippcivzla,true
158278844,fadlizon,true
79721594,alfredodelmazo,true
```

This tool can currently export around 10 million rows of this data from a 886 megabyte ORC file
(representing one day from 2020) in about 6 seconds:

```bash
$ time target/release/orcrs -vvv export --header --columns 0,3,9 /data/tsg/users/v2/2020-09-20.orc | wc
9705227 9705227 314287998

real    0m5.088s
user    0m6.048s
sys     0m0.349s
```

This is currently completely unoptimized and could be made at least a little faster.

## Features

This project currently only supports _reading_ ORC files
(writing will probably stay out of scope unless I switch to using bindings to the ORC C++ API at some point).

| Feature | Status | Notes |
|-|-|-|
| Integer types |:heavy_check_mark:| |
| String types |:heavy_check_mark:| |
| Floating point types |❌|Coming soon|
| Date types |❌| |
| Compound types |❌| |
| Zlib compression |:heavy_check_mark:| |
| Zstandard compression |:heavy_check_mark:| |
| Snappy compression |❌|Probably trivial|
| Column encryption |❌|Almost certainly permanently out of scope|

Also note that right now these tools don't use the indices: you see every row in the file.
So far this is fast enough for the things I need to do, but that will probably change in the future.

## Known issues

This software is largely untested, undocumented, and unoptimized.

## Developing

You'll need to install [Rust and Cargo][cargo] to build the project. Once you've got them, you can
check out this repository and run `cargo test` (to run the tests) and `cargo build --release` (to
build the command-line tool, which will be available as `target/release/orcrs`).

~~The [Protobuf schemas for the metadata in the ORC file][orc-proto] are not distributed with this
repository, but they will be downloaded to `$OUT_DIR/proto/` during the build. You can update this file
as needed either manually or by changing the commit in `build.rs`.~~ I got frustrated after 15 minutes
of trying to figure out how to make the Protobuf code generation work properly with the build, so it's
gone. You'll need to copy the `scripts/build.rs` file into the project directory in order to update the
Protobuf schemas (but this shouldn't be necessary very often).

This repository also includes a Java project with some code that I used for generating ORC test data
during development.

## Previous work

There's a partial implementation of a few pieces of an ORC reader for Rust [here][scritchley-orcrs].
I've borrowed a couple of test cases for the byte run length encoding reader, but my implementation
is otherwise unrelated.

## Future work

I'll probably continue to add support for ORC format features as I need them.
Eventually it'd be nice to have Rust bindings for the C++ API, and I may end up doing that here.

## License

This software is published under the [Anti-Capitalist Software License][acsl].

[acsl]: https://anticapitalist.software/
[apache-orc]: https://orc.apache.org/
[archive-team]: https://wiki.archiveteam.org/
[athena]: https://aws.amazon.com/athena
[cargo]: https://doc.rust-lang.org/cargo/getting-started/installation.html
[csv]: https://en.wikipedia.org/wiki/Comma-separated_values
[hadoop]: https://en.wikipedia.org/wiki/Apache_Hadoop
[hive]: https://en.wikipedia.org/wiki/Apache_Hive
[internet-archive]: https://archive.org/
[orc-cpp]: https://orc.apache.org/docs/core-cpp.html
[orc-proto]: https://github.com/apache/orc/blob/main/proto/orc_proto.proto
[orc-spec]: https://orc.apache.org/specification/ORCv1/
[rust]: https://www.rust-lang.org/
[s3]: https://aws.amazon.com/s3/
[scritchley-orcrs]: https://github.com/scritchley/orcrs
[stop-the-steal]: https://github.com/travisbrown/stop-the-steal
[twitter-stream-grab]: https://archive.org/details/twitterstream
[memory-lol]: https://twitter.com/travisbrown/status/1466414144261918721
