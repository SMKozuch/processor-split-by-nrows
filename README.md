# processor-split-by-nrows

Processor split by nrows for KBC.

The processor allows user to split a .csv file into multiple chunks with user-specified number of rows. The processor looks for a unique values and splits the dataset based on those.

### Usage
```
{
  "definition": {
    "component": "kozuch.processor-split-by-values"
  },
  "parameters": {
    "by_column": "split_column"
  }
}
```

### Output
.csv files, which are split based on unique values in column specified in the above config.
