
## Definitions:

* `Path(s)`: Tuple of (owner, repo, path) only of .js and .coffee files.
* `Original Commit`: Smallest timestamp commit for a file hash.
* `Buggy Paths`: Unmodified `path` which have copied from original commit, however, now original commit path has a different hash. (Original file is now modified).
* `Active Bugs`: Number of `Buggy paths` which have a javascript commit, anywhere in their repo, after the original file has been modified.


## Analysis Numbers:


|Item   				      | Quantity  	|Notes   							|
|---------------------|-------------|---------------------|
|Total Commits   		|9,299,054   	|   								|
|- Original Commits   	|2,303,762   	|   								|
|- Copy Commits   		|   			|   								|
|-- In different Path  	|2,001,415   	|   								|
|-- In same Path   		|Rest   		|Ignored in further analysis. Reverting file is the reason  	|
|					   	|   			|   								|
|Total Unique Paths 	|823,356    	|   								|
|- Buggy Paths			|123,158 (15%)	|   								|
|- Active Bugs		   	|79,379	(9.6%) 	|   								|
|- Copy Uniq Paths		|416,059 (50.5%)|   								|
|- Original Uniq Paths	|443,291 (53.8%)| Total can be greter than 100% as path can be counted in unique and copy if there are some modifications after copy.						|
