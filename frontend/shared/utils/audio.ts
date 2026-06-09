let successAudioContext: AudioContext | null = null;

function getAudioContext(): AudioContext | null {
  if (typeof window === "undefined") {
    return null;
  }

  if (successAudioContext && successAudioContext.state !== "closed") {
    return successAudioContext;
  }

  const audioContextCtor = window.AudioContext
    || (window as Window & { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
  if (!audioContextCtor) {
    return null;
  }

  successAudioContext = new audioContextCtor();
  return successAudioContext;
}

export async function primeSuccessSound(): Promise<void> {
  const audioContext = getAudioContext();
  if (!audioContext) {
    return;
  }
  if (audioContext.state === "suspended") {
    try {
      await audioContext.resume();
    } catch {
      return;
    }
  }
}

export async function playSuccessSound(): Promise<void> {
  const audioContext = getAudioContext();
  if (!audioContext) {
    return;
  }

  if (audioContext.state === "suspended") {
    try {
      await audioContext.resume();
    } catch {
      return;
    }
  }

  const now = audioContext.currentTime;
  const notes = [
    { frequency: 659.25, start: 0.0, duration: 0.14 },
    { frequency: 783.99, start: 0.12, duration: 0.14 },
    { frequency: 987.77, start: 0.24, duration: 0.18 },
  ];

  for (const note of notes) {
    const oscillator = audioContext.createOscillator();
    const gain = audioContext.createGain();
    const noteStart = now + note.start;
    const noteEnd = noteStart + note.duration;

    oscillator.type = "triangle";
    oscillator.frequency.setValueAtTime(note.frequency, noteStart);
    gain.gain.setValueAtTime(0.0001, noteStart);
    gain.gain.exponentialRampToValueAtTime(0.06, noteStart + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, noteEnd);

    oscillator.connect(gain);
    gain.connect(audioContext.destination);
    oscillator.start(noteStart);
    oscillator.stop(noteEnd + 0.02);
  }
}
