# Rule-Based Support Ticket Classifier

## Chosen Problem

This project classifies short technology support tickets into categories using rule-based keyword matching.
## Chosen Problem

The chosen problem is a rule-based support ticket classifier. The program reads support tickets from an input 
file and assigns each ticket to a category based on keyword matching rules. Each ticket contains a ticket ID and a short description 
of a problem someone might report to a help desk or technology support team. I chose this problem because it is practical, 
easy to understand, and detailed enough to show meaningful differences between Go and Prolog.

## Chosen Languages

Implementation 1: Go  
Implementation 2: Prolog

## Folder Structure

```text
final_project/
├── README.md
├── report.md
├── language1/
│   ├── main.go
│   ├── tickets.txt
│	└── expected_output.txt
└── language2/
    ├── classifier.pl
    ├── tickets.txt
	└── expected_output.txt
```

## Input Format

Each input line uses this format:

```text
ticket_id|ticket description
```

Example:

```text
1|Patron cannot log into their library account after resetting the password.
```


## Classification Categories

The programs should classify each ticket as one of these categories:

- password/account
- printing
- network/wifi
- hardware
- software
- urgent/security
- unknown

## Example Output

Ticket 1: password/account
Description: Patron cannot log into their library account after resetting the password.
Reason: matched words: password, account, log

Ticket 2: printing
Description: Printer is jammed and several print jobs are stuck in the queue.
Reason: matched words: printer, print, queue, jammed

Ticket 3: network/wifi
Description: Public computer has no internet connection and will not connect to WiFi.
Reason: matched words: wifi, internet, connection, connect

Ticket 4: hardware
Description: Staff laptop keyboard is not working, and the screen sometimes flickers.
Reason: matched words: keyboard, laptop, screen

Ticket 5: urgent/security
Description: Suspicious email is asking a staff member for login credentials.
Reason: matched words: suspicious, credentials

## Comparison Goal

The comparison is intended to show how Go handles the problem through explicit step-by-step processing, while Prolog handles the same problem through facts, rules, pattern matching, and logical inference.

## How to Run Implementation 1

To run **main.go**, move into the language1 folder from the cmd, ensure the text files are present, and run the command **go run main.go**

## How to Run Implementation 2

To run **classifier.pl**, move into the language2 folder from the cmd, ensure the tickets.txt, and expected_output.txt files are present, and run the command **swipl -q -s classifier.pl -g run -t halt**
