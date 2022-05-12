# sample_input_metadata
The *.py file presents a pySpark code used to process (extract needed information such as category and year of the video submission) the input files and output the result (combined information on year and category from many input files) into dataframe. 
The resulting dataframe was output into PostgreSQL table. Both PostgreSQL and Spark cluster were installed on different ec2 instances of AWS cloud.
The inputs to the pySpark code (extract_transform.py) were many small (Youtube video metadata)  *.txt files.
The _0veOqO-HM0.txt represents one of those input files.
