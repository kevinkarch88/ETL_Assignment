# Setup
Install Spark and postgres. Fill out your database information in main() and run.

# Write-up and long-term considerations

In two hours, I was able to the main program down but ran out of time for testing. If I were to test, I would use a few rows of sample data from each csv and unit test the separate functions.
I'd also test the database connection and have an integration test for the whole ETL process.

I added three metadata columns - timestamp, source file, and version number. I would increment the version number on each load. 

I used a 'column_map.json' to map the columns for each individual file. I'm not sure how scalable this is but it works for the sample.
If a file is loaded in a new format, I could use some sort of inference or pattern matching to figure out the column.

Logging would also be a good idea - I could log when the file format is wrong (like when a value spans 2 columns) and other problems with data types, db insertion, etc.
The same goes for errors. If I had more time I would have many more places that check for errors and throw exceptions.

By far the hardest part of this assignment was figuring out what to spend time on. With only a couple of hours, I focused mostly on the parsing side of things.

