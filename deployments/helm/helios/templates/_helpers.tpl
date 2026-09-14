{{/*
Common labels for Helios resources.
*/}}
{{- define "helios.labels" -}}
app.kubernetes.io/name: helios
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "helios.appLabels" -}}
app: {{ .name }}
app.kubernetes.io/part-of: helios
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .release }}
app.kubernetes.io/managed-by: {{ .managedBy }}
{{- end }}

{{- define "helios.busybox" -}}
{{ .Values.busybox.image }}
{{- end }}
