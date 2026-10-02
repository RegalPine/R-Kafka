name: Bug Report
description: Report a bug in R-Kafka
title: "[Bug]: "
labels: ["bug"]
body:
  - type: markdown
    attributes:
      value: |
        Thanks for reporting a bug! Please fill in the details below.

  - type: textarea
    id: description
    attributes:
      label: Bug Description
      description: A clear and concise description of the bug.
    validations:
      required: true

  - type: textarea
    id: steps
    attributes:
      label: Steps to Reproduce
      description: Steps to reproduce the behavior.
      placeholder: |
        1. Start R-Kafka with config '...'
        2. Send request '...'
        3. Observe '...'
    validations:
      required: true

  - type: textarea
    id: expected
    attributes:
      label: Expected Behavior
      description: What you expected to happen.
    validations:
      required: true

  - type: textarea
    id: actual
    attributes:
      label: Actual Behavior
      description: What actually happened. Include logs/error messages if available.
    validations:
      required: true

  - type: input
    id: version
    attributes:
      label: R-Kafka Version
      description: Which version/commit are you running?
    validations:
      required: true

  - type: input
    id: os
    attributes:
      label: Operating System
      description: e.g., Ubuntu 22.04, macOS 14.0
    validations:
      required: true

  - type: input
    id: rust-version
    attributes:
      label: Rust Version
      description: Output of `rustc --version`

  - type: textarea
    id: logs
    attributes:
      label: Logs / Stack Trace
      description: Please paste any relevant log output.
      render: shell
