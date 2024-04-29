{\rtf1\ansi\ansicpg936\cocoartf2759
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;\f1\fnil\fcharset0 HelveticaNeue;}
{\colortbl;\red255\green255\blue255;\red44\green34\blue176;}
{\*\expandedcolortbl;;\cssrgb\c23137\c22745\c74510;}
\paperw11900\paperh16840\margl1440\margr1440\vieww34000\viewh18780\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs36 \cf0 Categorize each video and put them into different output files.\
\
****************************\
Input:\
\pard\pardeftab720\partightenfactor0

\f1 \cf0 \cb2 \expnd0\expndtw0\kerning0
video ID an 11-digit string, which is unique\cb1 \uc0\u8232 \cb2 uploader a string of the video uploader's username\cb1 \uc0\u8232 \cb2 age an integer number of days between the date when the video was uploaded and Feb.15, 2007 (YouTube's establishment)\cb1 \uc0\u8232 \cb2 category a string of the video category chosen by the uploader\cb1 \uc0\u8232 \cb2 length an integer number of the video length\cb1 \uc0\u8232 \cb2 views an integer number of the views\cb1 \uc0\u8232 \cb2 rate a float number of the video rate\cb1 \uc0\u8232 \cb2 ratings an integer number of the ratings\cb1 \uc0\u8232 \cb2 comments an integer number of the comments\cb1 \uc0\u8232 \cb2 related IDs up to 20 strings of the related video IDs
\f0 \cb1 \kerning1\expnd0\expndtw0 \
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0
\cf0 \
Output:\
Same as Input in format, while videos in different categories are output into a file.\
}