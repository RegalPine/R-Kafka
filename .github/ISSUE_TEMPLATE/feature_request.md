name: Feature Request
description: Suggest a new feature for R-Kafka
title: "[Feature]: "
labels: ["enhancement"]
body:
  - type: markdown
    attributes:
      value: |
        Thanks for suggesting a feature! Please describe your idea.

  - type: textarea
    id: problem
    attributes:
      label: Problem / Motivation
      description: What problem does this feature solve? Why is it needed?
    validations:
      required: true

  - type: textarea
    id: solution
    attributes:
      label: Proposed Solution
      description: Describe the solution you'd like.
    validations:
      required: true

  - type: textarea
    id: alternatives
    attributes:
      label: Alternatives Considered
      description: Any alternative solutions or features you've considered?

  - type: textarea
    id: context
    attributes:
      label: Additional Context
      description: Any other context, references (KIPs, KEPs), or screenshots?

  - type: dropdown
    id: area
    attributes:
      label: Component
      description: Which component does this relate to?
      options:
        - Protocol / API
        - Storage Engine
        - Network
        - Broker
        - Replication
        - Controller (KRaft)
        - Security
        - Client SDK
        - Observability
        - Configuration
        - Other
    validations:
      required: true
