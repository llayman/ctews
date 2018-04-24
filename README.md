# CTEWS
Cyber Threat Early Warning System

Directory Layout

[Folder]ctews           ->Stores program files
    [file]twitter.py    ->Tweepy streaming service
    [file]db.py         ->Python mongo interface
[Folder]static          ->Stores javascript documents
[Folder]templates       ->Stores HTML documents
[Folder]venv            ->Stores enviornment info (Only compatible with linux)
[file]app.py            ->Main flask application
[file]csvGen.py         ->Generates CSV from mongo server
[file]collector.py      ->Creates a twitter streamer using config information
[file]ctews.wsgi        ->Points apache to the flask file and initiates the environment
[file]logging.yaml      ->Logging Format


