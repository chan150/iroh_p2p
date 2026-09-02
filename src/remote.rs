use anyhow::{Context, Result};
use image::codecs::jpeg::JpegEncoder;
use image::{ColorType, ImageEncoder};
use screenshots::Screen;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

/// 원격 제어 마우스/키보드 명령 프로토콜
#[derive(Debug, Clone, PartialEq)]
pub enum RemoteControlEvent {
    /// 마우스 정규화 좌표 이동 (0.0 ~ 1.0)
    MouseMove { x: f32, y: f32 },
    /// 마우스 버튼 다운
    MouseDown { button: MouseButton },
    /// 마우스 버튼 업
    MouseUp { button: MouseButton },
    /// 마우스 휠 스크롤
    MouseWheel { delta: i32 },
    /// 키보드 키 다운 (Virtual Key Code)
    KeyDown { key_code: u16 },
    /// 키보드 키 업
    KeyUp { key_code: u16 },
    /// 텍스트 직접 입력
    TextInput { text: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MouseButton {
    Left,
    Right,
    Middle,
}

impl RemoteControlEvent {
    pub fn serialize(&self) -> String {
        match self {
            Self::MouseMove { x, y } => format!("MM:{:.4}:{:.4}", x, y),
            Self::MouseDown { button } => format!("MD:{}", button.as_str()),
            Self::MouseUp { button } => format!("MU:{}", button.as_str()),
            Self::MouseWheel { delta } => format!("MW:{}", delta),
            Self::KeyDown { key_code } => format!("KD:{}", key_code),
            Self::KeyUp { key_code } => format!("KU:{}", key_code),
            Self::TextInput { text } => format!("TX:{}", text),
        }
    }

    pub fn deserialize(input: &str) -> Option<Self> {
        let parts: Vec<&str> = input.trim().splitn(3, ':').collect();
        if parts.is_empty() {
            return None;
        }

        match parts[0] {
            "MM" if parts.len() >= 3 => {
                let x = parts[1].parse::<f32>().ok()?;
                let y = parts[2].parse::<f32>().ok()?;
                Some(Self::MouseMove { x, y })
            }
            "MD" if parts.len() >= 2 => {
                let button = MouseButton::from_str(parts[1])?;
                Some(Self::MouseDown { button })
            }
            "MU" if parts.len() >= 2 => {
                let button = MouseButton::from_str(parts[1])?;
                Some(Self::MouseUp { button })
            }
            "MW" if parts.len() >= 2 => {
                let delta = parts[1].parse::<i32>().ok()?;
                Some(Self::MouseWheel { delta })
            }
            "KD" if parts.len() >= 2 => {
                let key_code = parts[1].parse::<u16>().ok()?;
                Some(Self::KeyDown { key_code })
            }
            "KU" if parts.len() >= 2 => {
                let key_code = parts[1].parse::<u16>().ok()?;
                Some(Self::KeyUp { key_code })
            }
            "TX" if parts.len() >= 2 => Some(Self::TextInput {
                text: parts[1..].join(":"),
            }),
            _ => None,
        }
    }
}

impl MouseButton {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Left => "L",
            Self::Right => "R",
            Self::Middle => "M",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "L" | "LEFT" => Some(Self::Left),
            "R" | "RIGHT" => Some(Self::Right),
            "M" | "MID" | "MIDDLE" => Some(Self::Middle),
            _ => None,
        }
    }
}

/// Windows 네이티브 입력 시뮬레이터 (마우스 & 키보드 조작)
pub struct WindowsInputSimulator {
    screen_width: i32,
    screen_height: i32,
}

impl WindowsInputSimulator {
    pub fn new() -> Self {
        #[cfg(target_os = "windows")]
        unsafe {
            use windows_sys::Win32::UI::WindowsAndMessaging::{
                GetSystemMetrics, SM_CXSCREEN, SM_CYSCREEN,
            };
            let width = GetSystemMetrics(SM_CXSCREEN);
            let height = GetSystemMetrics(SM_CYSCREEN);
            Self {
                screen_width: width.max(1),
                screen_height: height.max(1),
            }
        }
        #[cfg(not(target_os = "windows"))]
        {
            Self {
                screen_width: 1920,
                screen_height: 1080,
            }
        }
    }

    pub fn execute(&self, event: RemoteControlEvent) {
        #[cfg(target_os = "windows")]
        unsafe {
            use windows_sys::Win32::UI::Input::KeyboardAndMouse::*;
            use windows_sys::Win32::UI::WindowsAndMessaging::SetCursorPos;

            match event {
                RemoteControlEvent::MouseMove { x, y } => {
                    let target_x = (x.clamp(0.0, 1.0) * (self.screen_width as f32)) as i32;
                    let target_y = (y.clamp(0.0, 1.0) * (self.screen_height as f32)) as i32;
                    SetCursorPos(target_x, target_y);
                }
                RemoteControlEvent::MouseDown { button } => {
                    let flags = match button {
                        MouseButton::Left => MOUSEEVENTF_LEFTDOWN,
                        MouseButton::Right => MOUSEEVENTF_RIGHTDOWN,
                        MouseButton::Middle => MOUSEEVENTF_MIDDLEDOWN,
                    };
                    mouse_event(flags, 0, 0, 0, 0);
                }
                RemoteControlEvent::MouseUp { button } => {
                    let flags = match button {
                        MouseButton::Left => MOUSEEVENTF_LEFTUP,
                        MouseButton::Right => MOUSEEVENTF_RIGHTUP,
                        MouseButton::Middle => MOUSEEVENTF_MIDDLEUP,
                    };
                    mouse_event(flags, 0, 0, 0, 0);
                }
                RemoteControlEvent::MouseWheel { delta } => {
                    mouse_event(MOUSEEVENTF_WHEEL, 0, 0, delta, 0);
                }
                RemoteControlEvent::KeyDown { key_code } => {
                    keybd_event(key_code as u8, 0, 0, 0);
                }
                RemoteControlEvent::KeyUp { key_code } => {
                    keybd_event(key_code as u8, 0, KEYEVENTF_KEYUP, 0);
                }
                RemoteControlEvent::TextInput { text } => {
                    for ch in text.encode_utf16() {
                        keybd_event(ch as u8, 0, 0, 0);
                        keybd_event(ch as u8, 0, KEYEVENTF_KEYUP, 0);
                    }
                }
            }
        }
    }
}

/// 고속 화면 캡처 및 저지연 JPEG 압축 스트리머
pub struct ScreenStreamer {
    screen: Screen,
    quality: u8,
    fps: u32,
    running: Arc<AtomicBool>,
}

impl ScreenStreamer {
    pub fn new(display_index: usize, fps: u32, quality: u8) -> Result<Self> {
        let screens = Screen::all().context("모니터 목록 조회 실패")?;
        if screens.is_empty() {
            anyhow::bail!("감지된 모니터가 없습니다.");
        }
        let screen = screens
            .into_iter()
            .nth(display_index)
            .context("지정한 모니터를 찾을 수 없습니다.")?;

        Ok(Self {
            screen,
            quality: quality.clamp(30, 95),
            fps: fps.clamp(5, 60),
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        self.running.clone()
    }

    /// 화면 프레임을 지속적으로 캡처하여 QUIC 스트림으로 스트리밍 전송합니다.
    pub async fn start_stream(
        self,
        mut send_stream: iroh::endpoint::SendStream,
    ) -> Result<()> {
        let frame_interval = std::time::Duration::from_millis((1000 / self.fps) as u64);
        let mut frame_seq = 0u32;
        let mut jpeg_buffer = Vec::with_capacity(256 * 1024);

        // 헤더: [매직 4바이트 ("SCRN")]
        send_stream.write_all(b"SCRN").await.context("화면 스트림 헤더 전송 실패")?;

        println!(" 🖥️ [화면 공유 시작] 해상도: {}x{}, FPS: {}, 품질: {}%",
            self.screen.display_info.width,
            self.screen.display_info.height,
            self.fps,
            self.quality
        );

        while self.running.load(Ordering::Relaxed) {
            let start = std::time::Instant::now();

            // 1. 화면 캡처
            let image = match self.screen.capture() {
                Ok(img) => img,
                Err(e) => {
                    eprintln!(" [화면 캡처 오류]: {:?}", e);
                    tokio::time::sleep(frame_interval).await;
                    continue;
                }
            };

            let width = image.width();
            let height = image.height();
            let raw_rgba = image.as_raw();

            // 2. 고속 JPEG 인코딩
            jpeg_buffer.clear();
            let encoder = JpegEncoder::new_with_quality(&mut jpeg_buffer, self.quality);
            if let Err(e) = encoder.write_image(raw_rgba, width, height, ColorType::Rgba8.into()) {
                eprintln!(" [JPEG 인코딩 실패]: {:?}", e);
                continue;
            }

            // 3. 프레임 헤더 전송: [프레임 번호 (4바이트)] [너비 (2바이트)] [높이 (2바이트)] [JPEG 크기 (4바이트)]
            let jpeg_len = jpeg_buffer.len() as u32;
            let mut frame_hdr = [0u8; 12];
            frame_hdr[0..4].copy_from_slice(&frame_seq.to_le_bytes());
            frame_hdr[4..6].copy_from_slice(&(width as u16).to_le_bytes());
            frame_hdr[6..8].copy_from_slice(&(height as u16).to_le_bytes());
            frame_hdr[8..12].copy_from_slice(&jpeg_len.to_le_bytes());

            if let Err(e) = send_stream.write_all(&frame_hdr).await {
                eprintln!(" [화면 프레임 헤더 전송 실패]: {:?}", e);
                break;
            }

            if let Err(e) = send_stream.write_all(&jpeg_buffer).await {
                eprintln!(" [화면 프레임 데이터 전송 실패]: {:?}", e);
                break;
            }

            frame_seq = frame_seq.wrapping_add(1);

            let elapsed = start.elapsed();
            if elapsed < frame_interval {
                tokio::time::sleep(frame_interval - elapsed).await;
            }
        }

        let _ = send_stream.finish();
        println!(" 🖥️ [화면 공유 종료] 총 전송된 프레임: {}", frame_seq);
        Ok(())
    }
}

/// 수신된 화면 프레임 정보
pub struct ReceivedScreenFrame {
    pub frame_seq: u32,
    pub width: u16,
    pub height: u16,
    pub jpeg_data: Vec<u8>,
}

/// 화면 수신 리시버
pub async fn receive_screen_frame(
    recv_stream: &mut iroh::endpoint::RecvStream,
) -> Result<ReceivedScreenFrame> {
    let mut hdr = [0u8; 12];
    crate::read_exact_stream(recv_stream, &mut hdr).await.context("프레임 헤더 읽기 실패")?;

    let frame_seq = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
    let width = u16::from_le_bytes(hdr[4..6].try_into().unwrap());
    let height = u16::from_le_bytes(hdr[6..8].try_into().unwrap());
    let jpeg_len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;

    let mut jpeg_data = vec![0u8; jpeg_len];
    crate::read_exact_stream(recv_stream, &mut jpeg_data).await.context("프레임 데이터 읽기 실패")?;

    Ok(ReceivedScreenFrame {
        frame_seq,
        width,
        height,
        jpeg_data,
    })
}
