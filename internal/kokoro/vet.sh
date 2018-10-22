#!/bin/bash

# Fail on any error
set -eo pipefail

# Display commands being run
set -x

# Only run the linter on go1.11, since it needs type aliases (and we only care about its output once).
# TODO(deklerk) We should pass an environment variable from kokoro to decide this logic instead.
if [[ `go version` != *"go1.11"* ]]; then
    exit 0
fi

go get -u golang.org/x/lint/golint

# Runs the linter. Regrettably the linter is very simple and does not provide the ability to exclude rules or files,
# so we rely on inverse grepping to do this for us.
#
# Piping a bunch of greps may be slower than `grep -vE (thing|otherthing|anotherthing|etc)`, but since we have a good
# amount of things we're excluding, it seems better to optimize for readability.
#
# Note: since we added the linter after-the-fact, some of the ignored errors here are because we can't change an
# existing interface. (as opposed to us not caring about the error)
golint ./... 2>&1 | ( \
    grep -vE "gen\.go" | \
    grep -vE "receiver name [a-zA-Z]+[0-9]* should be consistent with previous receiver name" | \
    grep -vE "exported const AllUsers|AllAuthenticatedUsers|RoleOwner|SSD|HDD|PRODUCTION|DEVELOPMENT should have comment" | \
    grep -v "exported func Value returns unexported type pretty.val, which can be annoying to use" | \
    grep -v "ExecuteStreamingSql" | \
    grep -v "ClusterId" | \
    grep -v "InstanceId" | \
    grep -v "firestore.arrayUnion" | \
    grep -v "firestore.arrayRemove" | \
    grep -v "maxAttempts" | \
    grep -v "UptimeCheckIpIterator" | \
    grep -vE "apiv[0-9]+" | \
    grep -v "ALL_CAPS" | \
    grep -v "go-cloud-debug-agent" | \
    grep -v "mock_test" | \
    grep -v "a blank import should be only in a main or test package" | \
    grep -vE "\.pb\.go:" || true) | tee /dev/stderr | (! read)