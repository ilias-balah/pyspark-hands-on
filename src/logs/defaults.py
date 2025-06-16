# NOTE: ANSI escape codes for colors. For more information,
# see https://gist.github.com/fnky/458719343aabd01cfb17a3a4f7296797.
ANSI_COLORS = {
    "black":   { 'fg': "30", 'bg': "40" },
    "red":     { 'fg': "31", 'bg': "41" },
    "green":   { 'fg': "32", 'bg': "42" },
    "yellow":  { 'fg': "33", 'bg': "43" },
    "blue":    { 'fg': "34", 'bg': "44" },
    "magenta": { 'fg': "35", 'bg': "45" },
    "cyan":    { 'fg': "36", 'bg': "46" },
    "white":   { 'fg': "37", 'bg': "47" },
}

# Define a mapping of log levels to their corresponding ANSI colors
LOG_LEVELS_COLORS: dict[str, dict] = {
    'print':    ANSI_COLORS["black"],
    'debug':    ANSI_COLORS["cyan"],
    'info':     ANSI_COLORS["green"],
    'warning':  ANSI_COLORS["yellow"],
    'error':    ANSI_COLORS["red"],
    'critical': ANSI_COLORS["magenta"],
}

# ANSI escape character for formatting
ANSI_ESCAPE_CHARACTER = '\033[{}m'