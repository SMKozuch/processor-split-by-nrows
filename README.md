# processor-split-by-nrows

Processor split by nrows for KBC.

The processor allows user to split a .csv file into multiple chunks with user-specified number of rows. 

### Usage
```
{
  "definition": {
    "component": "kozuch.processor-split-by-nrows"
  },
  "parameters": {
    "nrows": 500
  }
}
```

### Output
.csv files which are split based on number of rows.
