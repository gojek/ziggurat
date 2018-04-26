# ziggurat

The actor framework for Project Lambda

## Usage
`[com.gojek.lambda/ziggurat "1.0.4"]`

## Configuration

All Ziggurat configs should be in your `clonfig` `config.edn` under the `:ziggurat` key.

## Gotchas
* Under the `proto3` format, it is no longer possible even to omit fields from a protobuf message. 
If a message field is set to the default value, it is omitted from the serialised message and will not 
be visible in the message passed to the mapper function. For example, if an integer value is set to `0`, which 
is the default, it will not be available at all in the message passed to the mapper function. Refer [here](https://developers.google.com/protocol-buffers/docs/proto3#default).

## Contribution

- For dev setup and contributions please refer to CONTRIBUTING.md
