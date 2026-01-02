{{- define "tail-sampling-selector.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tail-sampling-selector.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "tail-sampling-selector.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tail-sampling-selector.labels" -}}
helm.sh/chart: {{ include "tail-sampling-selector.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: tail-sampling-selector
app.kubernetes.io/name: {{ include "tail-sampling-selector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "tail-sampling-selector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tail-sampling-selector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "tail-sampling-selector.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "tail-sampling-selector.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "tail-sampling-selector.image" -}}
{{- $registry := .Values.global.image.repository | default .Values.image.repository }}
{{- $tag := .Values.global.image.tag | default .Values.image.tag }}
{{- if not $tag }}
{{- $tag = .Chart.AppVersion | default "latest" | toString }}
{{- end }}
{{- printf "%s:%s" $registry $tag }}
{{- end }}
