# Build

## Requirements

- [fletchgen]()
- [vhdmmio]()

### Fletchgen

```
fletchgen -n Sum -r example.rb -l vhdl --mmio64 --mmio-offset 64 --axi
```

### vhdmmio

> Limitation of fletchgen and vhdmmio require manual edits of the generated .mmio.yml file.
> These are included for this example in the sum.mmio.yml file. This step regenerates the mmio module.

```
vhdmmio -V vhdl -P vhdl sum.mmio.yml
```

### vhdeps

> Source file generation for OPAE is currently not supported. Manual edits are required.

# Synthesize
