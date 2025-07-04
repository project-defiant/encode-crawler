# Crawler for ENCODE data files

This repository contains a code for async download the ENCODE datasets based on the manifest file generated from the ENCODE portal.

The example manifest is located in `file_report_2025_7_2_14h_49m.tsv` file. The crawler will download the files to the `encode_dataset` directory.

## Running crawler

```{python}
uv run crawler --output-dir encode_dataset --input file_report_2025_7_2_14h_49m.tsv
```

## Checking if the files are correctly downloaded

In order to check if all files were correctly downloaded, we can check the file sizes against the sizes in the manifest file. This can be done with reruning the crawler command. In case the file is there and the size is the same, the file will be skipped.
