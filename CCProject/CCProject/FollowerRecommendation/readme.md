{\rtf1\ansi\ansicpg936\cocoartf2759
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;\f1\fnil\fcharset0 HelveticaNeue;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;\red45\green34\blue177;\red44\green34\blue176;
\red255\green255\blue255;}
{\*\expandedcolortbl;;\cssrgb\c0\c0\c0;\cssrgb\c23408\c23057\c74793;\cssrgb\c23137\c22745\c74510;
\cssrgb\c100000\c100000\c100000\c0;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs36 \cf0 Calculate the recommendation list of each video and output into files.\
****************************\
Mapper:\
Split the input tuples into <key,value> pairs. Key is the ID of each video, and the value is the IDs of other relevant  video.\
\
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0
\cf0 ****************************\
Reducer:\
Count the number of occurrences of their related videos and sort them according to the number of occurrences. Then the top 10 related video IDs with the highest number of occurrences are selected and output as a recommended list.\
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0
\cf0 \
Input:\
\pard\pardeftab720\partightenfactor0

\f1 \cf2 \cb3 \expnd0\expndtw0\kerning0
video ID an 11-digit string, which is unique\uc0\u8232 uploader a string of the video uploader's username\u8232 age an integer number of days between the date when the video was uploaded and Feb.15, 2007 (YouTube's establishment)\u8232 category a string of the video category chosen by the uploader\u8232 length an integer number of the video length\u8232 views an integer number of the views\u8232 rate a float number of the video rate\u8232 ratings an integer number of the ratings\u8232 comments an integer number of the comments\u8232 related IDs up to 20 strings of the related video IDs\cf0 \cb4 \
\
\cb5 Output:\
<key, value> \'97>key=ID, value=relevantID+relevantScore}