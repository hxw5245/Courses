# Activity: Street or Road? (DC eBook)

## Grading

##### The assignment is worth a total of 10 points.

- Turn in your HTML R Notebook in Canvas before the due date.
- [2 points] Work through the "Solved Example" section showing progress with each step
- "Back to the Streets":
    - [2 points] Your Turn #1: explain each line of code in English (either narrative or commented code)
    - [2 points] Your Turn #2: expand to match several more patterns (at least 12 total)
    - [2 points] Your Turn #2: provide a table in **descending order** of popularity for the street name identifiers you found
    - [2 points] Your Turn #2: use `ggplot` to construct a bar chart in **descending order** of popularity for the street name identifiers you found.

##### Assignment Remarks: 

Two data sets are provided.  One includes 15,000 street addresses of registered voters in Wake County, North Carolina.  The other includes over 900,000 street addresses of Medicare Service Providers.  You can use either data set (or both!) for the activity.

*Note: There's nothing to do in the "For the professional..." section at the very end except to be impressed.*

## Activity

People's addresses involve streets, lanes, courts, avenues, and so on.  How many such road-related words are in common use?

In general, the steps to automate this sort of task might be summarized as follows:

1. In everyday language, describe a pattern that you think will identify the information that you're looking for
2. Translate (1) into a regular expression
3. Filter to retain the cases that match the expression... hint: `filter()` and `grepl()` are useful for this
4. Filter to retain the cases that do not match the expression
5. Examine the results of (3) and (4) to identify shortcomings in your pattern matching algorithm
6. Improve or extend the pattern to deal with the mistaken cases
7. Repeat (1) through (6) until satisfied
8. Put extraction parenthesis around the parts of the regular expression that contain the information you want

