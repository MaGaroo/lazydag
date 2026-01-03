# LazyDAG
LazyDAG is a (yet another) python framework to manage data processing pipelines.

## Installation
Just how you install any other package nowadays:
```bash
pip install lazydag
```

## Usage
Take a look at the [examples](examples) directory for a usage example.

## Documentation
The best documentation is the source code itself.
No but seriously, the source code is not stable yet and it doesn't make sense
to put an effort in writing the documentation for it.

## FA(Philosophical)Q
### Why yet another framework?
I reviewed the existing tools, I didn't like them.
I decided to come up with my own.

### Why not use X?
Go ahead, use X.

### What's the story?
I needed to run some experiments for my research projects.
For the experiments, I needed to extract some information from some *entities*.
Sometimes, those entities had minor changes.
Sometimes, a small logic needed to change in my experiments' code.
Sometimes, I needed to recalculate some things I had already done before.
I don't like to wait for the whole pipeline to run again.
That's it. I created this framework to overcome those issues.
Maybe it will be useful for you.
Maybe it will be overfit to my use case.
But it's open source, so you can take a look and use it if you like it.
