import 'dart:convert';
import 'dart:collection';
import 'dart:io';
import 'dart:isolate';

const minusCharCode = 45;
const semicolonCharCode = 59;
const newlineCharCode = 10;
const maxLineLength = 100 + 1 + 4 + 1;
final _scratch = List<int>.filled(maxLineLength, 0);
final threadsCount = Platform.numberOfProcessors;

class Station {
  final String name;
  int min;
  int max;
  int sum;
  int count;

  Station(this.name, int value)
      : min = value,
        max = value,
        sum = value,
        count = 1;
}

Future<Map<int, Station>> readChunk(String filename, int start, int end) async {
  bool readingTemperature = false;

  final stationName = List<int>.filled(100, 0, growable: false);
  int stationNameLen = 0;

  final temperature = List<int>.filled(5, 0, growable: false);
  int temperatureLen = 0;

  int temp = 0;
  int key = 0;
  Station? stat;

  final stats = HashMap<int, Station>();
  await for (final chunk in File(filename).openRead(start, end)) {
    int c;
    for (int i = 0; i < chunk.length; i++) {
      c = chunk[i];

      switch (c) {
        case semicolonCharCode:
          readingTemperature = true;
          break;
        case newlineCharCode:
          // flush;
          temp = parseBufferIntoInt(temperature, temperatureLen);

          stat = stats[key];
          if (stat != null) {
            if (temp < stat.min) {
              stat.min = temp;
            }
            if (temp > stat.max) {
              stat.max = temp;
            }
            stat.sum += temp;
            stat.count += 1;
          } else {
            stats[key] = Station(
              utf8.decoder.convert(stationName, 0, stationNameLen),
              temp,
            );
          }

          readingTemperature = false;
          key = 0;
          stationNameLen = 0;
          temperatureLen = 0;
          break;
        default:
          // Otherwise expand buffers.
          if (readingTemperature) {
            temperature[temperatureLen++] = c;
          } else {
            key ^= (c + 0x9e3779b9 + (key << 4));
            stationName[stationNameLen++] = c;
          }
          break;
      }
    }
  }

  return stats;
}

void main(List<String> arguments) async {
  final filename = arguments[0];

  final file = File(filename).openSync();
  final size = file.lengthSync();
  final chunkSize = size ~/ threadsCount;

  int offset = 0;
  final chunkOffsets = List<int>.filled(threadsCount, 0, growable: false);
  for (int i = 0; i < threadsCount; ++i) {
    chunkOffsets[i] = offset;

    offset += chunkSize;

    file.setPositionSync(offset);
    file.readIntoSync(_scratch, 0, maxLineLength);

    final nlPos = _scratch.indexOf(newlineCharCode);
    assert(nlPos >= 0);
    offset += nlPos + 1;
  }

  final futures = List<Future<Map<int, Station>>>.generate(
    chunkOffsets.length,
    (int i) {
      final start = chunkOffsets[i];
      final end = i + 1 == chunkOffsets.length ? size : chunkOffsets[i + 1];
      return Isolate.run(() => readChunk(filename, start, end));
    },
    growable: false,
  );

  // Collect the results and merge
  final stats = HashMap<int, Station>();
  for (final result in await Future.wait(futures)) {
    for (final entry in result.entries) {
      final value = entry.value;
      final stat = stats[entry.key];
      if (stat == null) {
        stats[entry.key] = value;
        continue;
      }

      if (value.min < stat.min) stat.min = value.min;
      if (value.max > stat.max) stat.max = value.max;

      stat.sum += value.sum;
      stat.count += value.count;
    }
  }

  // Print the results.
  final s = stats.values.toList()..sort((a, b) => a.name.compareTo(b.name));
  String printStation(Station s) {
    final mean = (s.sum / (s.count * 10)).toStringAsFixed(1);
    return '${s.name}=${s.min / 10}/$mean/${s.max / 10}';
  }

  stdout.write('{${s.map(printStation).join(", ")}}\n');
}

// For a "-34.2" this will return -342.
int parseBufferIntoInt(final List<int> b, final int length) {
  if (b[0] == minusCharCode) {
    // b can be -1.1 or -11.1
    return -switch (length) {
      4 => parseOneDigit(b[1]) * 10 + parseOneDigit(b[3]),
      5 => parseOneDigit(b[1]) * 100 +
          parseOneDigit(b[2]) * 10 +
          parseOneDigit(b[4]),
      _ => 2048,
    };
  }

  // b can be 1.1 or 11.1
  return switch (length) {
    3 => parseOneDigit(b[0]) * 10 + parseOneDigit(b[2]),
    4 => parseOneDigit(b[0]) * 100 +
        parseOneDigit(b[1]) * 10 +
        parseOneDigit(b[3]),
    _ => -2048,
  };
}

int parseOneDigit(int char) => char - 0x30;
