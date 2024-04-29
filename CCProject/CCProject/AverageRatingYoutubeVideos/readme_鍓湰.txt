Calculate the mean of rate and the number of comments for each categories of the video.

*********************************
Mapper：
Split the input to get the category, rate and comments. Set category as key, set tuple class as value which contains information about comments and rate.



*********************************
Combiner:
Combine values whose keys are the same, and calculate the general rate and comments of each category.



*********************************
Reducer
Calculate the mean rate and general comments of each category.

Input:
video ID an 11-digit string, which is unique uploader a string of the video uploader's username age an integer number of days between the date when the video was uploaded and Feb.15, 2007 (YouTube's establishment) category a string of the video category chosen by the uploader length an integer number of the video length views an integer number of the views rate a float number of the video rate ratings an integer number of the ratings comments an integer number of the comments related IDs up to 20 strings of the related video IDs
Output:
(Key,value)—>value=(mean rate, comments)