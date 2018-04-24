# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## 1.0.5 - 2018-04-24
- Fixed a bug in application shutdown: the actor's start-fn was being called instead of the stop-fn.
- Made some functions private.
- Added some docstrings.
- Added Gotchas section to the README.

## 1.0.4 - 2018-04-20
### Added
- Added ziggurat.sentry/report-error to be used by actors.

## 1.0.3 - 2018-04-20
### Changed
- Upgrade esb-log-client version to latest (1.103.0).

## 1.0.2 - 2018-04-20
### Changed
- Various internal refactorings: removed dead code, fixed some spelling mistakes, made some functions private.

## 1.0.1 - 2018-04-18
### Added
- Flag to enable retries and conditionally start the rabbitmq states depending on this flag.

## 1.0.0 - 2018-04-17
### Changed
- Namespace framework configs under `:ziggurat`

## 0.1.0 - 2018-04-17
- Initial release
