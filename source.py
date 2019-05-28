#This is the source file where the user provides the search query to the system

from clean_slate_protocol import clean

if __name__ == "__main__":
    user_input = "And why is my music NEVER in my control center?! @AppleSupport" #input("Enter the support query to search: ")
    # Here the user input is structured into a query object  and fed to the 
    # clean slate protocol to get query tokens
    query_object = [
        (1, user_input)
    ]
    query_tokens = clean(query_object)
    print(query_tokens)