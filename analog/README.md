# SysML / SDG HDF5 analog

Generator: `generate_sdg_hdf5.py`. Fingerprint: **7 groups / 5 datasets / 56 attrs**, depth 4, `Values` int16.

CI default is `--n-series 8 --n-time 64` (not the 5001×10000 production-sized analog). K20 formula is still `floor(8 MiB / (n_time × 2))` (T=10000 → 419 rows).

```bash
just analog
just test
```

Run from a Nix/devenv shell so `h5py`/`numpy` resolve `libz`. Tests write files under pytest tmp; a committed binary fixture is optional.
