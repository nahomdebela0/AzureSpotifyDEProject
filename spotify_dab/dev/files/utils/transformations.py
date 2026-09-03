# This file is dedicated to deduplication and enhancing the reusability of functions across this project.
# Use this file to define common utility functions and procedures to avoid duplicate code.

class reusable:
    def dropColumns(self, df, columns):
        df = df.drop(*columns)
        return df