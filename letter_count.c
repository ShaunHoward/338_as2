#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAPPER_COUNT 26

/**
 * Opens the input file, reads and distributes
 * each line to a mapper which uses reducers
 * to count the number of occurrences of each
 * possible lower case letter in the file.
 * Returns 0 on success, 1 on error.
 */
int open_and_distribute_lines(){
	//read file and error check result
	FILE *input = fopen("input.txt", "r");
	if (input == NULL){
		perror("fopen");
		flush(stderr);
		return 1;
	}

	//create pipes
	int mappers[MAPPER_COUNT][2];
	//read lines and distribute via pipes to mappers
	size_t len = 0;
	ssize_t chars_read;
	char *line;
	while((chars_read=getline(&line, &len, input))!=-1){

	}

	//close file and free dynamic memory
	fclose(input);
	if (line){
		free(line);
	}
	return 0;
}

int main(void){
	//open file, read lines and distribute them across mappers
	if(open_and_distribute_lines() == 1)
		exit(EXIT_FAILURE);
	exit(EXIT_SUCCESS);
}
