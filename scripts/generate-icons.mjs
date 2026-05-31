import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const desktopBuildDir = path.join(root, 'apps', 'desktop', 'build');

const appIconSizes = [16, 24, 32, 48, 64, 128, 256];
const trayIconSizes = [16, 20, 24, 32, 40, 48];
const legacyMainIconPngBase64 =
  'iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAABGUlEQVR42u1XWw4DIQjk6BzNm9nYhixSYQcfSZvsJvxsUAYcByQiqogxcz3hSwnHU7Yli5WK/GgF/gAAv0u74ajmzryUUuXLrjX+88SSr4FZqMQau8UaCJoDATipQF6mAiLNi8hZNrUma3T21uRoAC7ERBsB1EF1xr0/o9yIs49Kfb/mQ9QtAHTWbWOPKxb0NABiQc9h1tZ0QK+K8C2QoF7wqyL9P6nQEgcQInkAOm1Y0QHLdlSiRTdAYfKzn+cA7xlIRrcgBgFdOwyAqFkX8OZIetA83gOVYv1pAPpILiC9NI+6ZVqKR3oeiZPXD4B5ITcFeW2XbcfEO2J+BvgqaeKWbBnJbCfMBt82kmVF6nkXPE+zJQCnnucvO69zimWss08AAAAASUVORK5CYII=';

fs.mkdirSync(desktopBuildDir, { recursive: true });

function decodePng(png) {
  let offset = 8;
  let width = 0;
  let height = 0;
  const chunks = [];

  while (offset < png.length) {
    const length = png.readUInt32BE(offset);
    const type = png.slice(offset + 4, offset + 8).toString('ascii');
    const data = png.slice(offset + 8, offset + 8 + length);
    if (type === 'IHDR') {
      width = data.readUInt32BE(0);
      height = data.readUInt32BE(4);
      if (data[8] !== 8 || data[9] !== 6) {
        throw new Error('Source icon PNG must be 8-bit RGBA.');
      }
    } else if (type === 'IDAT') {
      chunks.push(data);
    }
    offset += length + 12;
  }

  const raw = zlib.inflateSync(Buffer.concat(chunks));
  const rgba = Buffer.alloc(width * height * 4);
  const stride = width * 4;
  let source = 0;

  for (let y = 0; y < height; y += 1) {
    const filter = raw[source];
    if (filter !== 0) {
      throw new Error(`Unsupported PNG filter in source icon: ${filter}`);
    }
    source += 1;
    raw.copy(rgba, y * stride, source, source + stride);
    source += stride;
  }

  return { width, height, rgba };
}

const legacyMainIcon = decodePng(Buffer.from(legacyMainIconPngBase64, 'base64'));

function resizeNearest(image, size) {
  const output = Buffer.alloc(size * size * 4);

  for (let y = 0; y < size; y += 1) {
    const sourceY = Math.min(image.height - 1, Math.floor((y * image.height) / size));
    for (let x = 0; x < size; x += 1) {
      const sourceX = Math.min(image.width - 1, Math.floor((x * image.width) / size));
      const source = (sourceY * image.width + sourceX) * 4;
      const target = (y * size + x) * 4;
      image.rgba.copy(output, target, source, source + 4);
    }
  }

  return output;
}

function applyRoundedMask(rgba, size) {
  const output = Buffer.from(rgba);
  const radius = size * 0.24;
  const min = 0;
  const max = size - 1;

  for (let y = 0; y < size; y += 1) {
    for (let x = 0; x < size; x += 1) {
      const cx = Math.max(min + radius, Math.min(x, max - radius));
      const cy = Math.max(min + radius, Math.min(y, max - radius));
      if (Math.hypot(x - cx, y - cy) > radius) {
        output[(y * size + x) * 4 + 3] = 0;
      }
    }
  }

  return output;
}

function renderAppIcon(size) {
  return applyRoundedMask(resizeNearest(legacyMainIcon, size), size);
}

function renderTrayIcon(size) {
  return renderAppIcon(size);
}

function crc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc ^= byte;
    for (let i = 0; i < 8; i += 1) {
      crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function pngChunk(type, data) {
  const typeBuffer = Buffer.from(type, 'ascii');
  const chunk = Buffer.alloc(12 + data.length);
  chunk.writeUInt32BE(data.length, 0);
  typeBuffer.copy(chunk, 4);
  data.copy(chunk, 8);
  chunk.writeUInt32BE(crc32(Buffer.concat([typeBuffer, data])), 8 + data.length);
  return chunk;
}

function encodePng(width, height, rgba) {
  const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  ihdr[10] = 0;
  ihdr[11] = 0;
  ihdr[12] = 0;

  const stride = width * 4;
  const raw = Buffer.alloc((stride + 1) * height);
  for (let y = 0; y < height; y += 1) {
    raw[y * (stride + 1)] = 0;
    rgba.copy(raw, y * (stride + 1) + 1, y * stride, (y + 1) * stride);
  }

  return Buffer.concat([
    signature,
    pngChunk('IHDR', ihdr),
    pngChunk('IDAT', zlib.deflateSync(raw, { level: 9 })),
    pngChunk('IEND', Buffer.alloc(0))
  ]);
}

function encodeIco(images) {
  const headerSize = 6 + images.length * 16;
  let offset = headerSize;
  const header = Buffer.alloc(headerSize);
  header.writeUInt16LE(0, 0);
  header.writeUInt16LE(1, 2);
  header.writeUInt16LE(images.length, 4);

  images.forEach((image, index) => {
    const entry = 6 + index * 16;
    header[entry] = image.size === 256 ? 0 : image.size;
    header[entry + 1] = image.size === 256 ? 0 : image.size;
    header[entry + 2] = 0;
    header[entry + 3] = 0;
    header.writeUInt16LE(1, entry + 4);
    header.writeUInt16LE(32, entry + 6);
    header.writeUInt32LE(image.png.length, entry + 8);
    header.writeUInt32LE(offset, entry + 12);
    offset += image.png.length;
  });

  return Buffer.concat([header, ...images.map((image) => image.png)]);
}

function writePng(name, size, renderer) {
  const rgba = renderer(size);
  const png = encodePng(size, size, rgba);
  fs.writeFileSync(path.join(desktopBuildDir, name), png);
  return png;
}

function writeIco(name, sizes, renderer) {
  const images = sizes.map((size) => ({
    size,
    png: encodePng(size, size, renderer(size))
  }));
  fs.writeFileSync(path.join(desktopBuildDir, name), encodeIco(images));
}

writeIco('icon.ico', appIconSizes, renderAppIcon);
writeIco('tray.ico', trayIconSizes, renderTrayIcon);
writePng('tray.png', 32, renderTrayIcon);

console.log(`Generated icons in ${path.relative(root, desktopBuildDir)}`);
