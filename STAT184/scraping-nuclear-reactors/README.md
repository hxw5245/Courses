# Activity: Scraping Nuclear Reactors

## Instructions 

- Create a new R Notebook in this repo and add content similar to templates provided for other activities (you may also want to refer to the Style Guide Appendix in DataComputing eBook)
- Complete the Scraping Nuclear Reactors Activity found in the DataComputing eBook
    - make sure you carefully follow the instructions and mirror the code in the activity as you work 
    - **every task in the activity should have narrative text describing your observations; most steps also require code chunks and corresponding output.**
    - you should make commits in GitHub as you complete the activity
- submit a completed R Notebook as .NB.HTML to Canvas before deadline


## Tips & Errors in Book

- Wikipedia is a dynamic resource, and may have changed slightly since the activity was written
- **Your Turn**: Scrape Japan Data from Wikipedia
    - Hint: if you’re having trouble with “subscript out of bounds”, just get the xpath for the Japan table directly, rather than reading all tables at once
- **Your Turn**: Reconstruct Info-Graphic of Japan Reactors
    - Tip: it's fine to use `mutate(status_change = !is.na(status))` to plot a generic marker for "status change" rather than points with different shapes for each possible status


## Grading

Assignment is worth a total of 10 points.

- [2 points] Successfully scrape raw data for Japan Reactors from Wikipedia
- [2 points] Your Turn: Tidy Data & Data Cleaning (Japan)
- [2 points] Your Turn: Plot Net Generation Capacity vs Construction Date
- [2 points] Your Turn: Scrape & Clean China Data (then merge with Japan)
- [2 points] Your Turn: Reconstruct Info Graphic of Japan Reactors (or other country of interest)


