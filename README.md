# Match up EO data to tracking points

This code finds the closet in time EO variable for the coordinates/timestamps in the CSV file.

For each tracking point:
```mermaid
graph TD
    A(Tracking Point N) --> B[Get next closet in time acquisition cloud mask]
    B --> C{Is cloud free?}
    C --> |No| B
    C --> |Yes| D(Retrieve & store EO value) 
```

This is implemented in matchup_v1.py. 
matchup_v2.py does the same as matchup_v1.py, but uses the Sentinel Hub Statistics API, but it is slower, 
so use matchup_v1.py.


# Usage 

    python3 matchup_v1.py in.csv out.csv

# Modify EO variables 

See "evalscript_ndmi" in the matchup_v1.py for the JavaScript code that returns gets the EO value.
Modify it to get other EO variables.
