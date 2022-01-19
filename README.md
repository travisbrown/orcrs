# An ORC reader for Rust

[![Rust build status](https://img.shields.io/github/workflow/status/travisbrown/orcrs/rust-ci.svg?label=rust)](https://github.com/travisbrown/orcrs/actions)
[![Java build status](https://img.shields.io/github/workflow/status/travisbrown/orcrs/java-ci.svg?label=java)](https://github.com/travisbrown/orcrs/actions)
[![Coverage status](https://img.shields.io/codecov/c/github/travisbrown/orcrs/main.svg)](https://codecov.io/github/travisbrown/orcrs)

## Support

This project only supports reading ORC files.

We currently only support a few of ORC's scalar types (and none of the compound types):

- [x] boolean
- [x] tinyint
- [x] smallint
- [x] int
- [x] bigint
- [ ] float
- [ ] double
- [x] string
- [x] char
- [x] varchar
- [ ] binary
- [ ] timestamp
- [ ] date
- [ ] struct
- [ ] list
- [ ] map
- [ ] union

Support for all types is considered in-scope for the project, but will only be added as needed.

We do not currently support column encryption, and probably never will.

## Known issues

This software is largely untested and unoptimized.

## License

This software is published under the [Anti-Capitalist Software License][acsl].

[acsl]: https://anticapitalist.software/
[apache-orc]: https://orc.apache.org/
[orc-spec]: https://orc.apache.org/specification/ORCv1/
