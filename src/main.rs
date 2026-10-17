#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod engine;

use eframe::egui;
use engine::Progress;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

struct App {
    input: Option<PathBuf>,
    output: Option<PathBuf>,
    output_custom: bool,
    threshold_text: String,
    threads: usize,
    progress: Option<Arc<Progress>>,
}

impl Default for App {
    fn default() -> Self {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(16);
        App {
            input: None,
            output: None,
            output_custom: false,
            threshold_text: "0".into(),
            threads,
            progress: None,
        }
    }
}

impl App {
    fn set_input(&mut self, path: PathBuf) {
        if !self.output_custom {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("world");
            let out = path
                .parent()
                .map(|p| p.join(format!("{name}_thanos")))
                .unwrap_or_else(|| PathBuf::from(format!("{name}_thanos")));
            self.output = Some(out);
        }
        self.input = Some(path);
    }

    fn running(&self) -> bool {
        self.progress
            .as_ref()
            .map(|p| !p.finished.load(Ordering::SeqCst))
            .unwrap_or(false)
    }
}

fn fmt_bytes(b: u64) -> String {
    let b = b as f64;
    if b >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GB", b / 1024.0 / 1024.0 / 1024.0)
    } else if b >= 1024.0 * 1024.0 {
        format!("{:.1} MB", b / 1024.0 / 1024.0)
    } else {
        format!("{:.0} KB", b / 1024.0)
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Drag & Drop: Ordner ins Fenster ziehen
        if !self.running() {
            let dropped: Vec<PathBuf> = ctx.input(|i| {
                i.raw
                    .dropped_files
                    .iter()
                    .filter_map(|f| f.path.clone())
                    .collect()
            });
            if let Some(p) = dropped.into_iter().next() {
                if p.is_dir() {
                    self.set_input(p);
                }
            }
        }

        if self.running() {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Thanos-RS - Minecraft-Welt verkleinern");
            ui.add_space(6.0);
            ui.label("Entfernt Chunks, in denen kaum jemand war. Die Original-Welt bleibt unveraendert.");
            ui.add_space(10.0);

            let running = self.running();

            ui.group(|ui| {
                ui.label("1. Welt-Ordner (der Ordner mit level.dat)");
                ui.label(
                    self.input
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "Noch nichts gewaehlt - Ordner hierher ziehen".into()),
                );
                ui.add_enabled_ui(!running, |ui| {
                    if ui.button("Welt-Ordner waehlen ...").clicked() {
                        if let Some(p) = rfd::FileDialog::new().pick_folder() {
                            self.set_input(p);
                        }
                    }
                });
            });

            ui.add_space(6.0);
            ui.group(|ui| {
                ui.label("2. Ausgabe-Ordner (muss neu sein)");
                ui.label(
                    self.output
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "-".into()),
                );
                ui.add_enabled_ui(!running, |ui| {
                    if ui.button("Ausgabe-Ordner aendern ...").clicked() {
                        if let Some(base) = rfd::FileDialog::new().pick_folder() {
                            let name = self
                                .input
                                .as_ref()
                                .and_then(|p| p.file_name())
                                .and_then(|n| n.to_str())
                                .unwrap_or("world");
                            self.output = Some(base.join(format!("{name}_thanos")));
                            self.output_custom = true;
                        }
                    }
                });
            });

            ui.add_space(6.0);
            ui.group(|ui| {
                ui.label("3. Einstellungen");
                ui.add_enabled_ui(!running, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("Spielzeit-Grenze (Ticks, 20 Ticks = 1 Sekunde):");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.threshold_text)
                                .desired_width(90.0),
                        );
                    });
                    ui.label("Chunks mit Spielzeit bis zu diesem Wert werden geloescht. 0 = nie besucht.");
                    ui.horizontal(|ui| {
                        ui.label("Threads:");
                        ui.add(egui::Slider::new(&mut self.threads, 1..=32));
                    });
                });
            });

            ui.add_space(10.0);

            let threshold: Option<i64> = self.threshold_text.trim().parse().ok();
            let can_start = !running
                && self.input.is_some()
                && self.output.is_some()
                && threshold.is_some()
                && threshold.unwrap_or(-1) >= 0;

            ui.horizontal(|ui| {
                if ui
                    .add_enabled(can_start, egui::Button::new("Start"))
                    .clicked()
                {
                    let p = Arc::new(Progress::new());
                    self.progress = Some(p.clone());
                    let input = self.input.clone().unwrap();
                    let output = self.output.clone().unwrap();
                    let th = threshold.unwrap();
                    let threads = self.threads;
                    std::thread::spawn(move || {
                        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            engine::run(input, output, th, threads, &p);
                        }));
                        if r.is_err() {
                            if let Ok(mut e) = p.error.lock() {
                                *e = Some("Interner Fehler (Panic).".into());
                            }
                            p.finished.store(true, Ordering::SeqCst);
                        }
                    });
                }
                if running && ui.button("Abbrechen").clicked() {
                    if let Some(p) = &self.progress {
                        p.cancel.store(true, Ordering::SeqCst);
                    }
                }
            });
            if threshold.is_none() && !self.threshold_text.trim().is_empty() {
                ui.colored_label(egui::Color32::RED, "Spielzeit muss eine ganze Zahl >= 0 sein.");
            }

            if let Some(p) = &self.progress {
                ui.add_space(10.0);
                let total = p.total_files.load(Ordering::Relaxed);
                let done = p.done_files.load(Ordering::Relaxed);
                let frac = if total == 0 { 0.0 } else { done as f32 / total as f32 };
                ui.add(
                    egui::ProgressBar::new(frac)
                        .show_percentage()
                        .text(format!("{done} / {total} Region-Dateien")),
                );

                let before = p.bytes_before.load(Ordering::Relaxed);
                let after = p.bytes_after.load(Ordering::Relaxed);
                ui.label(format!(
                    "Chunks behalten: {}   entfernt: {}",
                    p.chunks_kept.load(Ordering::Relaxed),
                    p.chunks_removed.load(Ordering::Relaxed)
                ));
                ui.label(format!(
                    "Region-Daten vorher: {}   nachher: {}",
                    fmt_bytes(before),
                    fmt_bytes(after)
                ));

                if p.finished.load(Ordering::SeqCst) {
                    let err = p.error.lock().ok().and_then(|e| e.clone());
                    match err {
                        Some(e) => {
                            ui.colored_label(egui::Color32::RED, format!("Fehler: {e}"));
                        }
                        None => {
                            let saved = before.saturating_sub(after);
                            ui.colored_label(
                                egui::Color32::from_rgb(0, 160, 0),
                                format!("Fertig! Gespart: {}", fmt_bytes(saved)),
                            );
                        }
                    }
                }

                ui.add_space(6.0);
                egui::ScrollArea::vertical()
                    .max_height(140.0)
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        if let Ok(l) = p.log.lock() {
                            for line in l.iter() {
                                ui.label(line);
                            }
                        }
                    });
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([640.0, 640.0])
            .with_min_inner_size([520.0, 480.0])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        "Thanos-RS",
        options,
        Box::new(|_cc| Ok(Box::new(App::default()))),
    )
}