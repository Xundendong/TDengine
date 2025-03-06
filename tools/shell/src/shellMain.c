/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define __USE_XOPEN
#include "shellAuto.h"
#include "shellInt.h"

extern SShellObj shell;

void shellCrashHandler(int signum, void *sigInfo, void *context) {
  taosIgnSignal(SIGTERM);
  taosIgnSignal(SIGHUP);
  taosIgnSignal(SIGINT);
  taosIgnSignal(SIGBREAK);
  taosIgnSignal(SIGABRT);
  taosIgnSignal(SIGFPE);
  taosIgnSignal(SIGSEGV);
#if !defined(WINDOWS)
  taosIgnSignal(SIGBUS);
#endif

  taos_write_crashinfo(signum, sigInfo, context);

#ifdef _TD_DARWIN_64
  exit(signum);
#elif defined(WINDOWS)
  exit(signum);
#endif
}

// init arguments
void initArgument(SShellArgs *pArgs) {
  pArgs->host     = NULL;
  pArgs->port     = 0;
  pArgs->user     = NULL;
  pArgs->password = NULL;
  pArgs->database = NULL;

  // conn mode
  pArgs->dsn      = NULL;
  pArgs->connMode = CONN_MODE_INVALID;

  pArgs->port_inputted = false;
}

// set conn mode
int32_t setConnMode(int8_t connMode) {
  // set conn mode
  char * strMode = connMode == CONN_MODE_NATIVE ? STR_NATIVE : STR_WEBSOCKET;
  int32_t code = taos_options(TSDB_OPTION_DRIVER, strMode);
  if (code != TSDB_CODE_SUCCESS) {
    fprintf(stderr, "failed to load driver since %s [0x%08X]\r\n", taos_errstr(NULL), taos_errno(NULL));
    return -1;
  }
  return 0;
}

int main(int argc, char *argv[]) {
#if !defined(WINDOWS)
  taosSetSignal(SIGBUS, shellCrashHandler);
#endif
  taosSetSignal(SIGABRT, shellCrashHandler);
  taosSetSignal(SIGFPE, shellCrashHandler);
  taosSetSignal(SIGSEGV, shellCrashHandler);

  initArgument(&shell.args);

  if (shellCheckIntSize() != 0) {
    return -1;
  }

  if (shellParseArgs(argc, argv) != 0) {
    return -1;
  }

  if (shell.args.is_version) {
    shellPrintVersion();
    return 0;
  }

  if (shell.args.is_gen_auth) {
    shellGenerateAuth();
    return 0;
  }

  if (shell.args.is_help) {
    shellPrintHelp();
    return 0;
  }

  if (shell.args.netrole != NULL) {
    shellTestNetWork();
    return 0;
  }

  if (shell.args.is_dump_config) {
    shellDumpConfig();
    return 0;
  }

  if (getDsnEnv() != 0) {
    return -1;
  }

  if (setConnMode(shell.args.connMode)) {
    return -1;
  }

  if (taos_init() != 0) {
    fprintf(stderr, "failed to init shell since %s [0x%08X]\r\n", taos_errstr(NULL), taos_errno(NULL));
    return -1;
  }

  // kill heart-beat thread when quit
  taos_set_hb_quit(1);

  if (shell.args.is_startup || shell.args.is_check) {
    shellCheckServerStatus();
    taos_cleanup();
    return 0;
  }

  shellAutoInit();
  int32_t ret = shellExecute();
  shellAutoExit();

  return ret;
}
