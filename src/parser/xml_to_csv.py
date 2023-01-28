import xml.etree.ElementTree as Et


class XmlToCsv:
    """
    """
    def __init__(
            self,
            source_file: str,
            target_file: str,
            encoding: str = "utf-8",
            delimiter: str = ";"
    ) -> None:
        """
        Initializes the parser with paths to the input XML file and the output CSV file.
        """
        self._output_buffer: list[str] = []
        self._context = Et.iterparse(source_file, events=("start", "end"))
        self._delimiter = delimiter

        try:
            self._target = open(target_file, "w", encoding=encoding)
        except Exception as error:
            print(f"Failed to open the output file. Exception: {error}")
            raise

    def convert(
            self,
            root_tag: str,
            node_tags: list[str],
            leaf_tags: list[str],
            filter_node_tags: list[str],
            buffer_size: int = 1000,
            show_tags_in_node: bool = False
    ) -> None:
        """
        Converts the XRM file to CSV file.
        """
        try:
            header_columns = node_tags + leaf_tags
            self._output_buffer.append(f"{self._delimiter}".join(
                column for column in header_columns
            ))
            self._convert(
                root_tag,
                node_tags,
                leaf_tags,
                filter_node_tags,
                buffer_size,
                show_tags_in_node
            )
        except Exception as error:
            print(error)
        else:
            # Write rest from buffer to the target file
            self._write_buffer()
        finally:
            self._target.close()

    def _convert(
            self,
            root_tag: str,
            node_tags: list[str],
            leaf_tags: list[str],
            filter_node_tags: list[str],
            buffer_size: int,
            show_tags_in_node: bool
    ) -> None:
        header_columns = node_tags + leaf_tags
        record_data = {}
        tag_name = ""
        started = False

        for event, elem in self._context:
            elem: Et.Element
            tag = elem.tag.rpartition("}")[-1]  # Remove XML namespace from tags

            if event == "start":
                tag_name = ".".join((tag_name, tag)) if tag_name else tag

                if tag_name == root_tag and not started:
                    started = True
                    record_data = {column: "" for column in header_columns}
            else:
                elem_data = self._format_text(elem.text)

                if started and elem_data:
                    if tag_name in leaf_tags:
                        record_data[tag_name] = elem_data

                    if tag_name in filter_node_tags:
                        if show_tags_in_node:
                            elem_data = f"{tag_name}: {elem_data}" if elem_data else ""

                        for node_tag in filter(tag_name.startswith, node_tags):
                            node_data = record_data[node_tag]
                            node_data = f"{node_data}, {elem_data}" if node_data else elem_data
                            record_data[node_tag] = node_data

                if tag_name == root_tag and started:
                    started = False
                    self._output_buffer.append(f"{self._delimiter}".join(
                        record_data[column] for column in header_columns
                    ))

                tag_name = tag_name.rpartition('.' + tag)[0]
                elem.clear()

            # Flush buffer to disk
            if len(self._output_buffer) > buffer_size:
                self._write_buffer()

    def _write_buffer(self) -> None:
        """
        Writes records from buffer to the target file.
        """
        self._target.write("\n".join(self._output_buffer) + "\n")
        self.output_buffer = []

    @staticmethod
    def _format_text(text: str) -> str:
        return text.strip().replace("\n", " ").replace('"', r'""') if text else ""
