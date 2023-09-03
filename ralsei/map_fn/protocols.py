from typing import Callable, Iterator


OneToOne = Callable[..., dict]
"""
A function that maps one row to another

Example:
    ```python
    def download(url: str, page: int):
        response = requests.get(f"{url}/{page}")
        response.raise_for_status()
        return { "html": response.text }
    ```
"""


OneToMany = Callable[..., Iterator[dict]]
"""
A function that maps one row to multiple rows

Example:
    ```python
    def parse(html: str):
        sel = Selector(html)
        for row in sel.xpath("//table/tr"):
            yield {
                "name": sel.xpath("td[1]/text()").get(),
                "rank": sel.xpath("td[2]/text()").get(),
            }
    ```
"""
