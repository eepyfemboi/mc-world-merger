import os
from collections import defaultdict
from argparse import ArgumentParser


class Chunk:
    def __init__(self, timestamp, location, size, chunk_data):
        self.timestamp = timestamp
        self.location = location
        self.size = size
        self.chunk_data = chunk_data


class FilePair:
    def __init__(self, from_file, to_file):
        self.from_file = from_file
        self.to_file = to_file


class IMergeRule:
    def is_allowed_to_merge(self, current_chunk, new_chunk):
        pass


class MergeRuleAlways(IMergeRule):
    def is_allowed_to_merge(self, current_chunk, new_chunk):
        return True


class MergeRuleNever(IMergeRule):
    def is_allowed_to_merge(self, current_chunk, new_chunk):
        return False


class MergeRuleNewestChunks(IMergeRule):
    def is_allowed_to_merge(self, current_chunk, new_chunk):
        return new_chunk.timestamp > current_chunk.timestamp


class McaFile:
    sector_size = 4096

    def __init__(self, file_path):
        self.chunk_map = self.read_file(file_path)
        self.file_path = file_path

    def merge(self, other, rule):
        for pos, new_chunk in other.chunk_map.items():
            if pos in self.chunk_map:
                current_chunk = self.chunk_map[pos]
                if rule.is_allowed_to_merge(current_chunk, new_chunk):
                    self.chunk_map[pos] = new_chunk
            else:
                self.chunk_map[pos] = new_chunk

    def write(self, path):
        locations = bytearray(McaFile.sector_size)
        timestamps = bytearray(McaFile.sector_size)
        chunk_data_list = {}
        max_pos = [0]

        self.update_chunk_locations(self.chunk_map)

        for pos, chunk in self.chunk_map.items():
            self.set_location(locations, pos, chunk)
            self.set_timestamp(timestamps, pos, chunk)
            chunk_data_list[chunk.location] = chunk.chunk_data

            byte_position = (chunk.size + chunk.location - 2) * McaFile.sector_size
            if byte_position > max_pos[0]:
                max_pos[0] = byte_position

        to_write = self.join(locations, timestamps, chunk_data_list, max_pos[0])
        with open(path, "wb") as f:
            f.write(to_write)

    def update_chunk_locations(self, chunk_map):
        current_address = [2]
        for chunk in chunk_map.values():
            chunk.location = current_address[0]
            current_address[0] += chunk.size

    @staticmethod
    def set_chunk_data(chunk_data_list, chunk):
        chunk_data_list[chunk.location] = chunk.chunk_data

    @staticmethod
    def set_timestamp(timestamp, pos, chunk):
        timestamp[pos] = (chunk.timestamp >> 24) & 0xFF
        timestamp[pos + 1] = (chunk.timestamp >> 16) & 0xFF
        timestamp[pos + 2] = (chunk.timestamp >> 8) & 0xFF
        timestamp[pos + 3] = chunk.timestamp & 0xFF

    @staticmethod
    def set_location(locations, pos, chunk):
        locations[pos] = (chunk.location >> 16) & 0xFF
        locations[pos + 1] = (chunk.location >> 8) & 0xFF
        locations[pos + 2] = chunk.location & 0xFF
        locations[pos + 3] = chunk.size & 0xFF

    @staticmethod
    def join(locations, timestamps, data_list, max_pos):
        total_bytes = len(locations) + len(timestamps) + max_pos

        res = bytearray(total_bytes)
        res[: len(locations)] = locations
        res[McaFile.sector_size : McaFile.sector_size * 2] = timestamps

        for i, data in data_list.items():
            pos = i * McaFile.sector_size
            res[pos : pos + len(data)] = data

        return res

    @staticmethod
    def bytes_to_int(arr, start, end):
        res = 0
        while start <= end:
            res |= (arr[start] & 0xFF) << ((end - start) * 8)
            start += 1

        return res

    def read_file(self, mca_path):
        with open(mca_path, "rb") as f:
            bytes_data = f.read()

        locations = bytes_data[: McaFile.sector_size]
        timestamps = bytes_data[McaFile.sector_size : McaFile.sector_size * 2]
        chunk_data_array = bytes_data[McaFile.sector_size * 2 :]

        chunk_map = {}

        for i in range(0, len(locations), 4):
            timestamp = self.bytes_to_int(timestamps, i, i + 3)
            location = self.bytes_to_int(locations, i, i + 2)
            size = locations[i + 3] & 0xFF

            if size == 0:
                continue

            chunk_data_start = (location - 2) * McaFile.sector_size
            chunk_data_end = (location + size - 2) * McaFile.sector_size
            chunk_data = chunk_data_array[chunk_data_start:chunk_data_end]

            chunk_map[i] = Chunk(timestamp, location, size, chunk_data)

        return chunk_map


class RegionFinder:
    region_path = "region"

    def __init__(self, world1, world2):
        self.world1 = world1
        self.world2 = world2

    def merge_worlds(self, rule):
        self.merge_dimension("", rule)  # Merging the main "region" directory
        self.merge_dimension("DIM1", rule)  # Merging the "DIM1/region" directory
        self.merge_dimension("DIM-1", rule)  # Merging the "DIM-1/region" directory

    def merge_dimension(self, dimension, rule):
        world1_dim_path = os.path.join(self.world1, dimension, RegionFinder.region_path)
        world2_dim_path = os.path.join(self.world2, dimension, RegionFinder.region_path)

        files_to_copy = []
        files_to_merge = []

        f1 = self.list_mca_files(world1_dim_path)
        f2 = self.list_mca_files(world2_dim_path)

        for name, from_file, to_file in f2:
            if name in f1:
                files_to_merge.append(FilePair(from_file, f1[name]))
            else:
                files_to_copy.append(FilePair(from_file, os.path.join(world1_dim_path, name)))

        print(f"Region files in {os.path.join(dimension, RegionFinder.region_path)} to copy: {len(files_to_copy)}")
        for pair in files_to_copy:
            print(f"\tCopy: {pair.from_file}")

        print(f"Region files in {os.path.join(dimension, RegionFinder.region_path)} to merge: {len(files_to_merge)}")
        for pair in files_to_merge:
            print(f"\tMerge: {pair.to_file}")

        if files_to_copy or files_to_merge:
            user_input = input(f"Do you want to continue merging {dimension}? [Y/n]: ")
            if user_input.lower() == "y":
                self.copy(files_to_copy)
                self.merge(files_to_merge, rule)

    @staticmethod
    def copy(files):
        for pair in files:
            try:
                with open(pair.from_file, "rb") as f_in, open(pair.to_file, "wb") as f_out:
                    f_out.write(f_in.read())
            except Exception as e:
                print(f"Error copying {pair.from_file} to {pair.to_file}: {e}")

    def merge(self, files, rule):
        for pair in files:
            try:
                target = McaFile(pair.to_file)
                source = McaFile(pair.from_file)

                target.merge(source, rule)

                target.write(pair.to_file)
            except Exception as e:
                print(f"Error merging {pair.from_file} into {pair.to_file}: {e}")

    def list_mca_files(self, dim_path):
        map_files = defaultdict(list)

        for root, _, files in os.walk(dim_path):
            for file in files:
                if file.endswith(".mca") and os.path.getsize(os.path.join(root, file)) > 0:
                    map_files[file].append((file, os.path.join(root, file), os.path.join(self.world1, dim_path, file)))

        return [item for sublist in map_files.values() for item in sublist]


def main():
    parser = ArgumentParser(description="Merge two Minecraft worlds into one.")
    parser.add_argument("-r", "--rule", choices=["last-modified", "always", "never"], default="last-modified",
                        help="Set the method used to merge overlapping chunks.")
    parser.add_argument("--target_world", help="World to for chunks to be merged into. WARNING: chunks in this world may be overwritten.")
    parser.add_argument("--source_world", help="World to be merged from. Chunks in this world will remain unchanged.")
    args = parser.parse_args()

    rule_mapping = {
        "always": MergeRuleAlways(),
        "never": MergeRuleNever(),
        "last-modified": MergeRuleNewestChunks()
    }

    rule = rule_mapping[args.rule]
    finder = RegionFinder(args.target_world, args.source_world)
    finder.merge_worlds(rule)

    print("Done!")


if __name__ == "__main__":
    main()
