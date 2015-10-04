#!/bin/bash
if [ "$TRAVIS_BRANCH" == "master" ]; then
  sbt ++$TRAVIS_SCALA_VERSION publish
fi
