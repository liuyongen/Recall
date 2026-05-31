import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const desktopBuildDir = path.join(root, 'apps', 'desktop', 'build');

const appIconSizes = [16, 24, 32, 48, 64, 128, 256];
const trayIconSizes = [16, 20, 24, 32, 40, 48];
const supersample = 4;

fs.mkdirSync(desktopBuildDir, { recursive: true });

function hexToRgba(hex, alpha = 255) {
  const value = hex.replace('#', '');
  return [
    Number.parseInt(value.slice(0, 2), 16),
    Number.parseInt(value.slice(2, 4), 16),
    Number.parseInt(value.slice(4, 6), 16),
    alpha
  ];
}

function createCanvas(size) {
  const width = size * supersample;
  const height = size * supersample;
  return {
    size,
    width,
    height,
    data: new Uint8ClampedArray(width * height * 4)
  };
}

function blendPixel(canvas, x, y, color) {
  if (x < 0 || y < 0 || x >= canvas.width || y >= canvas.height || color[3] <= 0) {
    return;
  }

  const offset = (y * canvas.width + x) * 4;
  const srcA = color[3] / 255;
  const dstA = canvas.data[offset + 3] / 255;
  const outA = srcA + dstA * (1 - srcA);

  if (outA <= 0) {
    return;
  }

  canvas.data[offset] = Math.round((color[0] * srcA + canvas.data[offset] * dstA * (1 - srcA)) / outA);
  canvas.data[offset + 1] = Math.round((color[1] * srcA + canvas.data[offset + 1] * dstA * (1 - srcA)) / outA);
  canvas.data[offset + 2] = Math.round((color[2] * srcA + canvas.data[offset + 2] * dstA * (1 - srcA)) / outA);
  canvas.data[offset + 3] = Math.round(outA * 255);
}

function drawShape(canvas, color, contains) {
  for (let y = 0; y < canvas.height; y += 1) {
    for (let x = 0; x < canvas.width; x += 1) {
      const ux = (x + 0.5) / supersample;
      const uy = (y + 0.5) / supersample;
      if (contains(ux, uy, canvas.size)) {
        blendPixel(canvas, x, y, color);
      }
    }
  }
}

function drawEllipseStroke(canvas, options) {
  const { cx, cy, rx, ry, angle, stroke, color } = options;
  const cos = Math.cos(angle);
  const sin = Math.sin(angle);
  const strokeWidth = stroke / ((rx + ry) / 2);

  drawShape(canvas, color, (x, y) => {
    const dx = x - cx;
    const dy = y - cy;
    const localX = (dx * cos + dy * sin) / rx;
    const localY = (-dx * sin + dy * cos) / ry;
    return Math.abs(Math.hypot(localX, localY) - 1) <= strokeWidth / 2;
  });
}

function pointOnEllipse(cx, cy, rx, ry, angle, phase) {
  const cos = Math.cos(angle);
  const sin = Math.sin(angle);
  const x = rx * Math.cos(phase);
  const y = ry * Math.sin(phase);
  return [cx + x * cos - y * sin, cy + x * sin + y * cos];
}

function drawAtom(canvas, palette) {
  const { size } = canvas;
  const cx = size / 2;
  const cy = size / 2;
  const rx = size * 0.36;
  const ry = size * 0.135;
  const stroke = Math.max(1.15, size * 0.055);
  const orbitAngles = [0, Math.PI / 3, -Math.PI / 3];

  for (const angle of orbitAngles) {
    drawEllipseStroke(canvas, {
      cx,
      cy,
      rx,
      ry,
      angle,
      stroke,
      color: palette.orbitShadow
    });
  }

  for (const angle of orbitAngles) {
    drawEllipseStroke(canvas, {
      cx,
      cy,
      rx,
      ry,
      angle,
      stroke: stroke * 0.68,
      color: palette.orbit
    });
  }

  const electronRadius = Math.max(1.2, size * 0.052);
  const electronPhases = [0.18 * Math.PI, 0.86 * Math.PI, 1.53 * Math.PI];
  if (size >= 20) {
    orbitAngles.forEach((angle, index) => {
      const [x, y] = pointOnEllipse(cx, cy, rx, ry, angle, electronPhases[index]);
      drawShape(canvas, palette.electron, (px, py) => Math.hypot(px - x, py - y) <= electronRadius);
    });
  }

  drawShape(canvas, palette.core, (x, y) => Math.hypot(x - cx, y - cy) <= Math.max(1.8, size * 0.088));
}

function renderAppIcon(size) {
  const canvas = createCanvas(size);
  drawAtom(canvas, {
    orbitShadow: hexToRgba('#94A3B8', 210),
    orbit: hexToRgba('#F8FAFC', 255),
    electron: hexToRgba('#38BDF8', 245),
    core: hexToRgba('#22D3EE', 255)
  });
  return downsample(canvas);
}

function renderTrayIcon(size) {
  const canvas = createCanvas(size);
  drawAtom(canvas, {
    orbitShadow: hexToRgba('#94A3B8', 205),
    orbit: hexToRgba('#F8FAFC', 255),
    electron: hexToRgba('#E2E8F0', 245),
    core: hexToRgba('#F8FAFC', 255)
  });
  return downsample(canvas);
}

function downsample(canvas) {
  const { size, data, width } = canvas;
  const output = Buffer.alloc(size * size * 4);
  const samples = supersample * supersample;

  for (let y = 0; y < size; y += 1) {
    for (let x = 0; x < size; x += 1) {
      let r = 0;
      let g = 0;
      let b = 0;
      let a = 0;

      for (let sy = 0; sy < supersample; sy += 1) {
        for (let sx = 0; sx < supersample; sx += 1) {
          const source = (((y * supersample + sy) * width) + x * supersample + sx) * 4;
          const alpha = data[source + 3] / 255;
          r += data[source] * alpha;
          g += data[source + 1] * alpha;
          b += data[source + 2] * alpha;
          a += alpha;
        }
      }

      const target = (y * size + x) * 4;
      if (a > 0) {
        output[target] = Math.round(r / a);
        output[target + 1] = Math.round(g / a);
        output[target + 2] = Math.round(b / a);
      }
      output[target + 3] = Math.round((a / samples) * 255);
    }
  }

  return output;
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
