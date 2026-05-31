import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const desktopBuildDir = path.join(root, 'apps', 'desktop', 'build');

const appIconSizes = [16, 24, 32, 48, 64, 128, 256];
const trayIconSizes = [16, 20, 24, 32, 40, 48];
const supersample = 5;

fs.mkdirSync(desktopBuildDir, { recursive: true });

function rgba(hex, alpha = 255) {
  const value = hex.replace('#', '');
  return [
    Number.parseInt(value.slice(0, 2), 16),
    Number.parseInt(value.slice(2, 4), 16),
    Number.parseInt(value.slice(4, 6), 16),
    alpha
  ];
}

function createCanvas(size) {
  return {
    size,
    width: size * supersample,
    height: size * supersample,
    data: new Uint8ClampedArray(size * supersample * size * supersample * 4)
  };
}

function blendPixel(canvas, x, y, color) {
  if (x < 0 || y < 0 || x >= canvas.width || y >= canvas.height || color[3] <= 0) {
    return;
  }

  const offset = (y * canvas.width + x) * 4;
  const sourceAlpha = color[3] / 255;
  const targetAlpha = canvas.data[offset + 3] / 255;
  const outputAlpha = sourceAlpha + targetAlpha * (1 - sourceAlpha);

  if (outputAlpha <= 0) {
    return;
  }

  canvas.data[offset] = Math.round((color[0] * sourceAlpha + canvas.data[offset] * targetAlpha * (1 - sourceAlpha)) / outputAlpha);
  canvas.data[offset + 1] = Math.round((color[1] * sourceAlpha + canvas.data[offset + 1] * targetAlpha * (1 - sourceAlpha)) / outputAlpha);
  canvas.data[offset + 2] = Math.round((color[2] * sourceAlpha + canvas.data[offset + 2] * targetAlpha * (1 - sourceAlpha)) / outputAlpha);
  canvas.data[offset + 3] = Math.round(outputAlpha * 255);
}

function fillShape(canvas, color, contains) {
  for (let y = 0; y < canvas.height; y += 1) {
    for (let x = 0; x < canvas.width; x += 1) {
      const unitX = (x + 0.5) / supersample;
      const unitY = (y + 0.5) / supersample;
      if (contains(unitX, unitY, canvas.size)) {
        blendPixel(canvas, x, y, color);
      }
    }
  }
}

function fillRoundedRect(canvas, inset, radius, color) {
  fillShape(canvas, color, (x, y, size) => {
    const min = inset;
    const max = size - inset;
    const closestX = Math.max(min + radius, Math.min(x, max - radius));
    const closestY = Math.max(min + radius, Math.min(y, max - radius));
    return Math.hypot(x - closestX, y - closestY) <= radius;
  });
}

function strokeEllipse(canvas, options) {
  const { cx, cy, rx, ry, angle, width, color } = options;
  const cos = Math.cos(angle);
  const sin = Math.sin(angle);
  const normalizedWidth = width / ((rx + ry) / 2);

  fillShape(canvas, color, (x, y) => {
    const dx = x - cx;
    const dy = y - cy;
    const localX = (dx * cos + dy * sin) / rx;
    const localY = (-dx * sin + dy * cos) / ry;
    return Math.abs(Math.hypot(localX, localY) - 1) <= normalizedWidth / 2;
  });
}

function downsample(canvas) {
  const output = Buffer.alloc(canvas.size * canvas.size * 4);
  const samples = supersample * supersample;

  for (let y = 0; y < canvas.size; y += 1) {
    for (let x = 0; x < canvas.size; x += 1) {
      let red = 0;
      let green = 0;
      let blue = 0;
      let alpha = 0;

      for (let sampleY = 0; sampleY < supersample; sampleY += 1) {
        for (let sampleX = 0; sampleX < supersample; sampleX += 1) {
          const source = (((y * supersample + sampleY) * canvas.width) + x * supersample + sampleX) * 4;
          const sampleAlpha = canvas.data[source + 3] / 255;
          red += canvas.data[source] * sampleAlpha;
          green += canvas.data[source + 1] * sampleAlpha;
          blue += canvas.data[source + 2] * sampleAlpha;
          alpha += sampleAlpha;
        }
      }

      const target = (y * canvas.size + x) * 4;
      if (alpha > 0) {
        output[target] = Math.round(red / alpha);
        output[target + 1] = Math.round(green / alpha);
        output[target + 2] = Math.round(blue / alpha);
      }
      output[target + 3] = Math.round((alpha / samples) * 255);
    }
  }

  return output;
}

function renderRecallMark(canvas, options = {}) {
  const {
    scale = 1,
    strokeScale = 1,
    shadow = true,
    centerColor = rgba('#21C7D9')
  } = options;
  const size = canvas.size;
  const cx = size * 0.5;
  const cy = size * 0.5;
  const orbitRx = size * 0.315 * scale;
  const orbitRy = size * 0.128 * scale;
  const orbitWidth = Math.max(1.15, size * 0.058 * strokeScale);
  const dotRadius = Math.max(0.9, size * 0.06 * scale);
  const orbitAngles = [0, Math.PI / 3, -Math.PI / 3];

  if (shadow) {
    for (const angle of orbitAngles) {
      strokeEllipse(canvas, {
        cx,
        cy,
        rx: orbitRx,
        ry: orbitRy,
        angle,
        width: orbitWidth * 1.58,
        color: rgba('#374151', 230)
      });
    }
  }

  for (const angle of orbitAngles) {
    strokeEllipse(canvas, {
      cx,
      cy,
      rx: orbitRx,
      ry: orbitRy,
      angle,
      width: orbitWidth,
      color: rgba('#F8FAFC')
    });
  }

  fillShape(canvas, centerColor, (x, y) => Math.hypot(x - cx, y - cy) <= dotRadius);
}

function renderMainIcon(size) {
  const canvas = createCanvas(size);
  const inset = size <= 24 ? 1 : size * 0.045;
  const radius = size * 0.24;

  fillRoundedRect(canvas, inset, radius, rgba('#05070A'));
  renderRecallMark(canvas);

  return downsample(canvas);
}

function renderAppIcon(size) {
  return renderMainIcon(size);
}

function renderTrayIcon(size) {
  return renderMainIcon(size);
}

function assertTrayMatchesMainShape() {
  const main = renderAppIcon(32);
  const tray = renderTrayIcon(32);
  if (!main.equals(tray)) {
    throw new Error('Tray icon must be pixel-identical to the 32px main icon.');
  }
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

assertTrayMatchesMainShape();

writeIco('icon.ico', appIconSizes, renderAppIcon);
writeIco('tray.ico', trayIconSizes, renderTrayIcon);
writePng('tray.png', 32, renderTrayIcon);

console.log(`Generated icons in ${path.relative(root, desktopBuildDir)}`);
