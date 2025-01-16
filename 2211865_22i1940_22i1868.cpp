#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;

// ---------------------------------------------
// Configuration Constants
// ---------------------------------------------
#define MAX_WORDS 100
#define MAX_WORD_LEN 50
#define MAX_KEYVAL_PAIRS 200

// ---------------------------------------------
// Data Structures
// ---------------------------------------------
struct keyval_t
{
    char key[MAX_WORD_LEN];
    int value;
};

struct SharedData
{
    keyval_t intermediate[MAX_KEYVAL_PAIRS];
    int intermediate_count;
    pthread_mutex_t intermediate_mutex;
    sem_t map_done_sem;
};

struct reducer_arg_t
{
    char key[MAX_WORD_LEN];
    int start_index;
    int end_index;
    SharedData *shared; // pointer to shared data
};

// ---------------------------------------------
// Helper Functions
// ---------------------------------------------
bool is_punctuation(char c)
{
    // Treat any non-alphanumeric and non-whitespace character as punctuation
    if ((c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') ||
        c == ' ' || c == '\t')
    {
        return false;
    }
    return true;
}

void remove_punctuation(char *line)
{
    int j = 0;
    for (int i = 0; line[i] != '\0'; i++)
    {
        if (!is_punctuation(line[i]))
        {
            // Keep non-punctuation characters
            line[j++] = line[i];
        }
        else
        {
            // Skip punctuation entirely
        }
    }
    line[j] = '\0'; // Null-terminate the modified string
}

int split_into_words(char *input, char words[][MAX_WORD_LEN])
{
    int count = 0;
    char *token = strtok(input, " ");
    while (token != NULL && count < MAX_WORDS)
    {
        strncpy(words[count], token, MAX_WORD_LEN - 1);
        words[count][MAX_WORD_LEN - 1] = '\0';
        count++;
        token = strtok(NULL, " ");
    }
    return count;
}

void insert_intermediate_pair(SharedData *shared, const char *key, int value)
{
    pthread_mutex_lock(&shared->intermediate_mutex);
    if (shared->intermediate_count < MAX_KEYVAL_PAIRS)
    {
        strncpy(shared->intermediate[shared->intermediate_count].key, key, MAX_WORD_LEN - 1);
        shared->intermediate[shared->intermediate_count].key[MAX_WORD_LEN - 1] = '\0';
        shared->intermediate[shared->intermediate_count].value = value;
        shared->intermediate_count++;
    }
    pthread_mutex_unlock(&shared->intermediate_mutex);
}

// Insertion sort by key
void insertion_sort(keyval_t arr[], int n)
{
    for (int i = 1; i < n; i++)
    {
        keyval_t temp = arr[i];
        int j = i - 1;
        while (j >= 0 && strcmp(arr[j].key, temp.key) > 0)
        {
            arr[j + 1] = arr[j];
            j--;
        }
        arr[j + 1] = temp;
    }
}

// Shuffler: sorts intermediate results by key
void shuffler(SharedData *shared)
{
    cout << "\n----------------------------------------\n";
    cout << "              Shuffle Phase\n";
    cout << "----------------------------------------\n";
    cout << "Shuffling and sorting intermediate results...\n";
    usleep(500000); // simulate some processing time

    insertion_sort(shared->intermediate, shared->intermediate_count);

    cout << "Shuffling complete. Intermediate results (sorted by key):\n";
    for (int i = 0; i < shared->intermediate_count; i++)
    {
        cout << "  (\"" << shared->intermediate[i].key << "\", " << shared->intermediate[i].value << ")\n";
    }
}

// Mapper process: reads from read_fd and writes (key,value) to write_fd
void mapper_process(int read_fd, int write_fd)
{
    dup2(read_fd, STDIN_FILENO);
    close(read_fd);
    dup2(write_fd, STDOUT_FILENO);
    close(write_fd);

    char buffer[MAX_WORD_LEN];
    while (scanf("%s", buffer) != EOF)
    {
        // Simulate processing delay
        usleep(100000);
        printf("%s %d\n", buffer, 1);
        fflush(stdout);
    }

    exit(0);
}

// Reducer thread function
void *reducer_thread(void *arg)
{
    reducer_arg_t *rarg = (reducer_arg_t *)arg;
    SharedData *shared = rarg->shared;

    int sum = 0;
    for (int i = rarg->start_index; i <= rarg->end_index; i++)
    {
        sum += shared->intermediate[i].value;
    }

    // Print each final count
    cout << "  " << rarg->key << " : " << sum << "\n";
    free(rarg);
    return NULL;
}

// ---------------------------------------------
// Main
// ---------------------------------------------
int main()
{
    SharedData shared;
    shared.intermediate_count = 0;

    pthread_mutex_init(&shared.intermediate_mutex, NULL);
    sem_init(&shared.map_done_sem, 0, 0);

    cout << "============================================================\n";
    cout << "         Welcome to the Word Count MapReduce Program\n";
    cout << "============================================================\n";
    cout << "Please enter your input text (multi-line allowed). Press CTRL+D when done:\n\n";

    ios::sync_with_stdio(false);
    cin.tie(NULL);

    string input_all;
    {
        char line[1024];
        while (true)
        {
            if (!cin.good())
                break;
            if (!cin.getline(line, sizeof(line)))
                break;
            // Append this line and a space to separate lines
            input_all += line;
            input_all += ' ';
        }
    }

    if (input_all.empty())
    {
        cerr << "No input provided.\n";
        return 1;
    }

    char input_str[4096];
    strncpy(input_str, input_all.c_str(), sizeof(input_str) - 1);
    input_str[sizeof(input_str) - 1] = '\0';

    // Remove punctuation
    remove_punctuation(input_str);

    char words[MAX_WORDS][MAX_WORD_LEN];
    char input_copy[sizeof(input_str)];
    strncpy(input_copy, input_str, sizeof(input_str));
    input_copy[sizeof(input_str) - 1] = '\0';

    int total_words = split_into_words(input_copy, words);
    if (total_words == 0)
    {
        cerr << "No words found after removing punctuation.\n";
        return 1;
    }

    cout << "\n----------------------------------------\n";
    cout << "             Input Summary\n";
    cout << "----------------------------------------\n";
    cout << "Number of words extracted: " << total_words << "\n";
    cout << "Words identified:\n";
    for (int i = 0; i < total_words; i++)
    {
        cout << "  " << words[i] << "\n";
    }

    // Decide number of mappers based on total_words
    // For example: 1 mapper per 20 words, minimum 1 mapper
    int num_mappers = (total_words + 19) / 20; // rounds up division by 20
    if (num_mappers < 1)
        num_mappers = 1;

    cout << "\nNumber of mappers selected: " << num_mappers << "\n";

    // Divide words among mappers dynamically
    int chunk_size = (total_words / num_mappers);
    int remainder = (total_words % num_mappers);

    // Dynamically create arrays for pipes based on num_mappers
    int **in_pipes = new int *[num_mappers];
    int **out_pipes = new int *[num_mappers];
    for (int i = 0; i < num_mappers; i++)
    {
        in_pipes[i] = new int[2];
        out_pipes[i] = new int[2];
    }

    for (int i = 0; i < num_mappers; i++)
    {
        if (pipe(in_pipes[i]) == -1 || pipe(out_pipes[i]) == -1)
        {
            perror("pipe");
            return 1;
        }
    }

    cout << "\n----------------------------------------\n";
    cout << "             Mapping Phase\n";
    cout << "----------------------------------------\n";
    cout << "Splitting work among " << num_mappers << " mapper(s)...\n";

    int word_index = 0;
    for (int i = 0; i < num_mappers; i++)
    {
        int current_chunk = chunk_size + ((i < remainder) ? 1 : 0);

        pid_t pid = fork();
        if (pid < 0)
        {
            perror("fork");
            return 1;
        }

        if (pid == 0)
        {
            // Child mapper
            close(in_pipes[i][1]);
            close(out_pipes[i][0]);
            mapper_process(in_pipes[i][0], out_pipes[i][1]);
        }
        else
        {
            // Parent
            close(in_pipes[i][0]);
            close(out_pipes[i][1]);

            cout << "\nMapper " << i << " processing words:\n";
            for (int w = 0; w < current_chunk; w++)
            {
                cout << "  Sending word: \"" << words[word_index + w] << "\" to mapper " << i << "\n";
                dprintf(in_pipes[i][1], "%s\n", words[word_index + w]);
            }
            word_index += current_chunk;
            close(in_pipes[i][1]);
        }
    }

    cout << "\nWaiting for mappers to finish...\n";
    // Read intermediate pairs from mappers
    for (int i = 0; i < num_mappers; i++)
    {
        FILE *fp = fdopen(out_pipes[i][0], "r");
        if (!fp)
        {
            perror("fdopen");
            return 1;
        }

        cout << "\nCollecting results from Mapper " << i << "...\n";
        char keybuf[MAX_WORD_LEN];
        int val;
        while (fscanf(fp, "%s %d", keybuf, &val) == 2)
        {
            cout << "  Received intermediate pair: (\"" << keybuf << "\", " << val << ")\n";
            insert_intermediate_pair(&shared, keybuf, val);
        }
        fclose(fp);
    }

    for (int i = 0; i < num_mappers; i++)
    {
        wait(NULL);
    }

    // Shuffle intermediate results
    shuffler(&shared);

    // Signal map done
    sem_post(&shared.map_done_sem);

    cout << "\n----------------------------------------\n";
    cout << "             Reducing Phase\n";
    cout << "----------------------------------------\n";
    cout << "Aggregating counts for each unique word...\n";

    pthread_t reducers[MAX_KEYVAL_PAIRS];
    int reducer_count = 0;

    int start = 0;
    while (start < shared.intermediate_count)
    {
        int end = start;
        while (end + 1 < shared.intermediate_count &&
               strcmp(shared.intermediate[end + 1].key, shared.intermediate[start].key) == 0)
        {
            end++;
        }

        reducer_arg_t *rarg = (reducer_arg_t *)malloc(sizeof(reducer_arg_t));
        if (!rarg)
        {
            perror("malloc");
            return 1;
        }
        strncpy(rarg->key, shared.intermediate[start].key, MAX_WORD_LEN - 1);
        rarg->key[MAX_WORD_LEN - 1] = '\0';
        rarg->start_index = start;
        rarg->end_index = end;
        rarg->shared = &shared;

        if (pthread_create(&reducers[reducer_count], NULL, reducer_thread, (void *)rarg) != 0)
        {
            perror("pthread_create");
            return 1;
        }
        reducer_count++;
        start = end + 1;
    }

    // Wait for all reducers
    for (int i = 0; i < reducer_count; i++)
    {
        pthread_join(reducers[i], NULL);
    }

    cout << "\n----------------------------------------\n";
    cout << "           Final Word Counts\n";
    cout << "----------------------------------------\n";
    cout << "(Listed above are the aggregated results.)\n";

    cout << "\nProcessing complete. All reducers have finished.\n";
    cout << "Thank you for using this MapReduce simulation!\n\n";

    // Cleanup
    pthread_mutex_destroy(&shared.intermediate_mutex);
    sem_destroy(&shared.map_done_sem);

    // Deallocate pipe arrays
    for (int i = 0; i < num_mappers; i++)
    {
        delete[] in_pipes[i];
        delete[] out_pipes[i];
    }
    delete[] in_pipes;
    delete[] out_pipes;

    return 0;
}
