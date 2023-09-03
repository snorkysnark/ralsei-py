You declare your pipeline as a dictionary
in a pipeline factory function that receives [argparse.Namespace][],
the cli arguments, as its input.

```py
def make_pipeline(args):
    return {
        "make_urls": MapToNewTable(...),
        "download": MapToNewColumns(...),
        "extract": CreateTableSql(...),
    }
```

In the example above, 3 named tasks are created,
as well as a hidden `__full__` sequence that runs those tasks
in the order they were defined.

If needed, you can explicitly define `__full__`
as well as other sequences. Let's say that there are 2 variants
of the `extract` task: one for the older
and one the newer version of a website.

```python
def make_pipeline(args):
    return {
        "make_urls": MapToNewTable(...),
        "download": MapToNewColumns(...),
        "extract1": AddColumnsSql(...),
        "extract2": CreateTableSql(...),

        "old": [
            "make_urls",
            "download",
            "extract1"
        ],
        "__full__": [
            "make_urls",
            "download",
            "extract2"
        ]
    }
```

- So, you could run `script.py __full__ run` for the new version,
- `script.py old run` for the old version,
- or run any of the tasks individially.

When running a sequence,
it only executes the tasks that haven't been done yet.

!!! warning
    Note that the pipeline at the moment does **not** resolve dependencies
    and put tasks in the correct order.

    They simply run in the order they are defined.
