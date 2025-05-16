# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed
- Queue order function now takes into account potential victims, resulting in better reclaim scenarios.

### CHANGED
- Cached GetDeservedShare and GetFairShare function in the scheduler PodGroupInfo to improve performance
- Added cache to the binder resource reservation client
