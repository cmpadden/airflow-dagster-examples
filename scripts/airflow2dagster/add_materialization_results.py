import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable

relevant_documentation = """
### Metadata and Markdown

The DataFrame was embedded into the asset's metadata with Markdown. Any valid Markdown snippet can be stored and rendered in the Dagster UI, including images. By embedding a bar chart of the most frequently used words as metadata, you and your team can visualize and analyze the `most_frequent_words` asset without leaving the Dagster UI.

Below is code that changes shows how to add an an image of a bar chart in asset metadata. Replace your `most_frequent_words` asset with the following:

```python file=/tutorial/building_an_asset_graph/assets_with_metadata.py startafter=start_most_frequent_words_asset_with_metadata endbefore=end_most_frequent_words_asset_with_metadata
@asset(deps=[topstories])
def most_frequent_words() -> MaterializeResult:
    stopwords = ["a", "the", "an", "of", "to", "in", "for", "and", "with", "on", "is"]

    topstories = pd.read_csv("data/topstories.csv")

    # loop through the titles and count the frequency of each word
    word_counts = {}
    for raw_title in topstories["title"]:
        title = raw_title.lower()
        for word in title.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in stopwords and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    # Get the top 25 most frequent words
    top_words = {
        pair[0]: pair[1]
        for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
    }

    # Make a bar chart of the top 25 words
    plt.figure(figsize=(10, 6))
    plt.bar(list(top_words.keys()), list(top_words.values()))
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 25 Words in Hacker News Titles")
    plt.tight_layout()

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    with open("data/most_frequent_words.json", "w") as f:
        json.dump(top_words, f)

    # Attach the Markdown content as metadata to the asset
    return MaterializeResult(metadata={"plot": MetadataValue.md(md_content)})
```
"""


class AddMaterializationResultSignature(dspy.Signature):
    """
    Surface relevant metadata using `MaterializeResult` instead of `AssetMaterialization`.

    Ensure that metadata fields are added using `MetadataValue`.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField()
    dagster_code = dspy.OutputField(
        desc="Dagster code with `MaterializeResult` surfacing asset metadata where relevant"
    )


class AddMaterializationResultModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_materialization = dspy.ChainOfThought(
            AddMaterializationResultSignature
        )

    def forward(self, input_dagster_code: str) -> dspy.Prediction:
        # context = self.retrieve(self.add_materialization.signature.__doc__).passages
        context = [relevant_documentation]
        pred = self.add_materialization(
            context="\n".join(context), input_dagster_code=input_dagster_code
        )
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )

        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(output=pred.dagster_code)
