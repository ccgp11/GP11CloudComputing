Calculate the recommendation list of each video and output into files.
****************************
Mapper:
Split the input tuples into <key,value> pairs. Key is the ID of each video, and the value is the IDs of other relevant  video.

****************************
Reducer:
Count the number of occurrences of their related videos and sort them according to the number of occurrences. Then the top 10 related video IDs with the highest number of occurrences are selected and output as a recommended list.

Input:
video ID an 11-digit string, which is unique uploader a string of the video uploader's username age an integer number of days between the date when the video was uploaded and Feb.15, 2007 (YouTube's establishment) category a string of the video category chosen by the uploader length an integer number of the video length views an integer number of the views rate a float number of the video rate ratings an integer number of the ratings comments an integer number of the comments related IDs up to 20 strings of the related video IDs

Output:
<key, value> —>key=ID, value=relevantID+relevantScore