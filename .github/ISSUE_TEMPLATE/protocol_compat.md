name: Protocol Compatibility
description: Report a Kafka protocol compatibility issue
title: "[Protocol]: "
labels: ["protocol-compat"]
body:
  - type: markdown
    attributes:
      value: |
        Report a protocol compatibility issue between R-Kafka and Apache Kafka clients/brokers.

  - type: input
    id: api-key
    attributes:
      label: API Key / Name
      description: e.g., Produce (0), Fetch (1), JoinGroup (11)
    validations:
      required: true

  - type: input
    id: api-version
    attributes:
      label: API Version
      description: Which API version is affected?
    validations:
      required: true

  - type: dropdown
    id: direction
    attributes:
      label: Direction
      description: Which direction is the incompatibility?
      options:
        - "Java Client → R-Kafka Broker"
        - "R-Kafka Client → Java Broker"
        - "R-Kafka Broker ↔ Java Broker (cluster mix)"
        - "Other"
    validations:
      required: true

  - type: textarea
    id: description
    attributes:
      label: Issue Description
      description: Describe the incompatibility. Include error messages, packet captures, or hex dumps if available.
    validations:
      required: true

  - type: input
    id: kafka-version
    attributes:
      label: Apache Kafka Version
      description: Which Kafka client/broker version was used for testing?
      placeholder: e.g., 3.7.0

  - type: textarea
    id: reference
    attributes:
      label: Protocol Reference
      description: Link to the relevant Kafka protocol documentation or KIP.
