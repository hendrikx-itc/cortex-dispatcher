Configuration
=============

Cortex Dispatcher is configured using a `YAML <http://yaml.org/>`_ based
configuration file. Here is an example configuration:

.. code-block:: yaml

    sources:
      - directory: /test-data
        targets:
          - regex: "^.*-red\\.xml$"
            directory: /test-red
          - regex: "^.*-blue\\.xml$"
            directory: /test-blue

