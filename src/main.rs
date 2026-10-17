#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod engine;

use eframe::egui::{self, Color32, FontId, Margin, RichText, Rounding, Stroke, Vec2};
use engine::Progress;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

const BG: Color32 = Color32::from_rgb(20, 22, 27);
const CARD: Color32 = Color32::from_rgb(31, 34, 42);
const CARD_BORDER: Color32 = Color32::from_rgb(45, 49, 60);
const ACCENT: Color32 = Color32::from_rgb(0, 190, 145);
const ACCENT_HOVER: Color32 = Color32::from_rgb(0, 220, 170);
const TEXT: Color32 = Color32::from_rgb(235, 238, 244);
const MUTED: Color32 = Color32::from_rgb(150, 158, 172);
const DANGER: Color32 = Color32::from_rgb(230, 85, 85);
const OK: Color32 = Color32::from_rgb(0, 200, 110);
const BTN_GREY: Color32 = Color32::from_rgb(55, 60, 72);

struct App {
    input: Option<PathBuf>,
    output: Option<PathBuf>,
    threshold_text: String,
    threads: usize,
    make_zip: bool,
    progress: Option<Arc<Progress>>,
    zip_after: bool,
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
            threshold_text: "0".into(),
            threads,
            make_zip: false,
            progress: None,
            zip_after: false,
        }
    }
}

fn card(ui: &mut egui::Ui, add: impl FnOnce(&mut egui::Ui)) {
    egui::Frame::none()
        .fill(CARD)
        .stroke(Stroke::new(1.0, CARD_BORDER))
        .rounding(Rounding::same(14.0))
        .inner_margin(Margin::same(18.0))
        .outer_margin(Margin::symmetric(0.0, 6.0))
        .show(ui, add);
}

fn step_title(ui: &mut egui::Ui, num: &str, title: &str) {
    ui.horizontal(|ui| {
        ui.label(RichText::new(num).font(FontId::proportional(20.0)).color(ACCENT).strong());
        ui.label(RichText::new(title).font(FontId::proportional(20.0)).color(ACCENT).strong());
    });
    ui.add_space(6.0);
}

fn button(ui: &mut egui::Ui, text: &str, fill: Color32, enabled: bool) -> egui::Response {
    let btn = egui::Button::new(
        RichText::new(text).font(FontId::proportional(15.0)).color(TEXT),
    )
        .fill(fill)
        .rounding(Rounding::same(10.0))
        .min_size(Vec2::new(0.0, 34.0));
    ui.add_enabled(enabled, btn)
}

fn fmt_bytes(b: u64) -> String {
    let f = b as f64;
    if f >= 1_073_741_824.0 {
        format!("{:.2} GB", f / 1_073_741_824.0)
    } else if f >= 1_048_576.0 {
        format!("{:.1} MB", f / 1_048_576.0)
    } else {
        format!("{:.0} KB", f / 1024.0)
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let mut visuals = egui::Visuals::dark();
        visuals.panel_fill = BG;
        visuals.window_fill = BG;
        visuals.widgets.inactive.bg_fill = BTN_GREY;
        visuals.widgets.hovered.bg_fill = ACCENT_HOVER;
        visuals.selection.bg_fill = ACCENT;
        ctx.set_visuals(visuals);

        let running = self
            .progress
            .as_ref()
            .map(|p| !p.finished.load(Ordering::SeqCst))
            .unwrap_or(false);
        if running {
            ctx.request_repaint();
        }

        egui::CentralPanel::default()
            .frame(egui::Frame::none().fill(BG).inner_margin(Margin::same(22.0)))
            .show(ctx, |ui| {
                ui.label(RichText::new("THANOS-RS").font(FontId::proportional(34.0)).color(ACCENT).strong());
                ui.label(
                    RichText::new("Entfernt Chunks, in denen kaum jemand war. Original bleibt unveraendert.")
                        .font(FontId::proportional(14.0))
                        .color(MUTED),
                );
                ui.add_space(10.0);

                // 1. Welt-Ordner
                card(ui, |ui| {
                    step_title(ui, "1", "Welt-Ordner (Ordner mit level.dat)");
                    let txt = self
                        .input
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "Ordner hierher ziehen oder waehlen".into());
                    ui.label(RichText::new(txt).font(FontId::proportional(14.0)).color(MUTED));
                    ui.add_space(8.0);
                    if button(ui, "Welt-Ordner waehlen ...", BTN_GREY, !running).clicked() {
                        if let Some(p) = rfd::FileDialog::new().pick_folder() {
                            self.output = p.parent().map(|par| {
                                par.join(format!(
                                    "{}_thanos",
                                    p.file_name().unwrap_or_default().to_string_lossy()
                                ))
                            });
                            self.input = Some(p);
                        }
                    }
                });

                // 2. Ausgabe-Ordner
                card(ui, |ui| {
                    step_title(ui, "2", "Ausgabe-Ordner (muss neu sein)");
                    let txt = self
                        .output
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "-".into());
                    ui.label(RichText::new(txt).font(FontId::proportional(14.0)).color(MUTED));
                    ui.add_space(8.0);
                    if button(ui, "Ausgabe-Ordner aendern ...", BTN_GREY, !running).clicked() {
                        if let Some(p) = rfd::FileDialog::new().pick_folder() {
                            self.output = Some(p);
                        }
                    }
                });

                // 3. Einstellungen
                card(ui, |ui| {
                    step_title(ui, "3", "Einstellungen");
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Spielzeit-Grenze (Ticks, 20 = 1 Sek.):").font(FontId::proportional(14.0)).color(TEXT));
                        ui.add_enabled(
                            !running,
                            egui::TextEdit::singleline(&mut self.threshold_text).desired_width(120.0),
                        );
                    });
                    ui.label(
                        RichText::new("0 = nur nie besuchte Chunks entfernen (sicher)")
                            .font(FontId::proportional(12.0))
                            .color(MUTED),
                    );
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Threads:").font(FontId::proportional(14.0)).color(TEXT));
                        ui.add_enabled(
                            !running,
                            egui::Slider::new(&mut self.threads, 1..=16),
                        );
                    });
                    ui.add_space(4.0);
                    ui.add_enabled(
                        !running,
                        egui::Checkbox::new(&mut self.make_zip, "Ergebnis zusaetzlich als ZIP packen"),
                    );
                });

                ui.add_space(8.0);

                // START
                ui.horizontal(|ui| {
                    let can_start = !running && self.input.is_some() && self.output.is_some();
                    let start = egui::Button::new(
                        RichText::new("START").font(FontId::proportional(18.0)).color(Color32::BLACK).strong(),
                    )
                        .fill(if can_start { ACCENT } else { BTN_GREY })
                        .rounding(Rounding::same(12.0))
                        .min_size(Vec2::new(200.0, 46.0));
                    if ui.add_enabled(can_start, start).clicked() {
                        let threshold: i64 = self.threshold_text.trim().parse().unwrap_or(0);
                        let p = Arc::new(Progress::new());
                        self.progress = Some(p.clone());
                        self.zip_after = self.make_zip;
                        let input = self.input.clone().unwrap();
                        let output = self.output.clone().unwrap();
                        let threads = self.threads;
                        let zip = self.make_zip;
                        std::thread::spawn(move || {
                            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                engine::run(input, output, threshold, threads, &p)
                            }));
                            match r {
                                Ok(Some(final_out)) => {
                                    if zip {
                                        let _ = engine::zip_dir(&final_out, &p);
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    if let Ok(mut e) = p.error.lock() {
                                        *e = Some("Interner Fehler (Panic).".into());
                                    }
                                    p.finished.store(true, Ordering::SeqCst);
                                }
                            }
                        });
                    }
                    if running && button(ui, "Abbrechen", DANGER, true).clicked() {
                        if let Some(p) = &self.progress {
                            p.cancel.store(true, Ordering::SeqCst);
                        }
                    }
                });

                // Fortschritt & Ergebnis
                if let Some(p) = self.progress.clone() {
                    ui.add_space(8.0);
                    card(ui, |ui| {
                        let total = p.total_files.load(Ordering::Relaxed).max(1);
                        let done = p.done_files.load(Ordering::Relaxed);
                        let frac = done as f32 / total as f32;
                        ui.add(
                            egui::ProgressBar::new(frac)
                                .fill(ACCENT)
                                .text(format!("{done} / {total} Region-Dateien")),
                        );
                        ui.add_space(6.0);

                        let before = p.bytes_before.load(Ordering::Relaxed);
                        let after = p.bytes_after.load(Ordering::Relaxed);
                        ui.label(
                            RichText::new(format!(
                                "Chunks behalten: {}   entfernt: {}",
                                p.chunks_kept.load(Ordering::Relaxed),
                                p.chunks_removed.load(Ordering::Relaxed)
                            ))
                                .color(TEXT),
                        );
                        ui.label(
                            RichText::new(format!(
                                "Region-Daten vorher: {}   nachher: {}",
                                fmt_bytes(before),
                                fmt_bytes(after)
                            ))
                                .color(TEXT),
                        );

                        if p.finished.load(Ordering::SeqCst) {
                            ui.add_space(6.0);
                            let err = p.error.lock().ok().and_then(|e| e.clone());
                            match err {
                                Some(e) => {
                                    ui.label(RichText::new(format!("Fehler: {e}")).color(DANGER).strong());
                                }
                                None => {
                                    let saved = before.saturating_sub(after);
                                    ui.label(
                                        RichText::new(format!("Fertig! Gespart: {}", fmt_bytes(saved)))
                                            .color(OK)
                                            .strong()
                                            .font(FontId::proportional(16.0)),
                                    );
                                }
                            }
                        }
                    });

                    // Log
                    card(ui, |ui| {
                        ui.label(RichText::new("Log").color(ACCENT).strong());
                        egui::ScrollArea::vertical()
                            .max_height(150.0)
                            .stick_to_bottom(true)
                            .show(ui, |ui| {
                                if let Ok(l) = p.log.lock() {
                                    for line in l.iter() {
                                        ui.label(RichText::new(line).font(FontId::monospace(12.0)).color(MUTED));
                                    }
                                }
                            });
                    });
                }
            });
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([720.0, 860.0])
            .with_title("Thanos-RS")
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        "Thanos-RS",
        options,
        Box::new(|cc| {
            let mut style = (*cc.egui_ctx.style()).clone();
            style.spacing.item_spacing = Vec2::new(10.0, 10.0);
            style.spacing.button_padding = Vec2::new(14.0, 8.0);
            cc.egui_ctx.set_style(style);
            Ok(Box::new(App::default()))
        }),
    )
}